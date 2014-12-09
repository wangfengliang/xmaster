#!/usr/bin/env python
#coding=utf-8

# Copyright (c) Twisted Matrix Laboratories.
# See LICENSE for details.

import json
import ConfigParser

from twisted.internet.protocol import Protocol, Factory
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor

from statscol import StatsCollector, MStatsCollector
from list_all_members import list_all_members, list_pb_all_numbers

from UrlTaskContainer import UrlTaskContainer
from mylogger import Logger
from message_pb2 import *

import redis

class SpiderInfos(object):
    def __init__(self):
        self.stats = MStatsCollector()

    def update(self, ping):
        spider_id = ping.id
        for key, value in list_pb_all_numbers(ping):
            self.stats.set_value(spider_id, key, value)
        self.stats._print(spider_id)

class MasterServer(LineReceiver):
    def __init__(self):
        self.master_name = g_config.get('master', 'name')
        level = g_config.get('master', 'level') if g_config.has_option('master', 'level') else "DEBUG"
        debug = g_config.getboolean('master', 'debug') if g_config.has_option('master', 'debug') else True
        logfile = g_config.get('master', 'logfile') if g_config.has_option('master', 'logfile') else None
        logname = g_config.get('master', 'logname') if g_config.has_option('master', 'logname') else None
        if not debug:
            assert logfile, 'logfile must be set when not debug mode'
        self.logger = Logger.getLogger(logname, logfile, level=level, debug=debug)

        self.spider_status = SpiderInfos()
        redis_host = g_config.get('master', 'redis_addr') if g_config.has_option('master', 'redis_addr') else 'localhost'
        redis_port = g_config.getint('master', 'redis_port') if g_config.has_option('master', 'redis_port') else 6379
        self.redis_instance = redis.Redis(host=redis_host, port=redis_port, db=0)
        self.rqst_manager = UrlTaskContainer(self.master_name, use_cache=True, redis_instance=self.redis_instance, logger=self.logger)

    def connectionMade(self):
        self.logger.info('connectionMade')

    def lineReceived(self, line):
        print 'dataReceived', line

        # 解析协议
        msg = Message()
        msg.ParseFromString(line)
        if msg.type == Message.PING:
            self.logger.info('receive PING message!')
            ping = Ping()
            ping.ParseFromString(msg.body)

            self.spider_status.update(ping) # 记录每个节点的状态

            pong = Pong()
            pong.time = ping.time
            msg.type = Message.PONG
            msg.body = pong.SerializeToString()
            msgstr = msg.SerializeToString()
            self.sendLine(msgstr)
        elif msg.type == Message.REQ_TASK:
            self.logger.info('receive REQ_TASK message')
            task_req = TaskRequest()
            task_req.ParseFromString(msg.body)
            exist_hosts_d = {}
            for host_ in task_req.host_infos:
                exist_hosts_d[host_.key] = host_.value

            def _send_task_reply_(self, rqsts):
                task_rep = TaskReply()
                task_rep.id = task_req.id
                task_rep.tasks.extend(rqsts)
                msg.type = Message.REP_TASK
                msg.body = task_rep.SerializeToString()
                msgstr = msg.SerializeToString()
                self.sendLine(msgstr)

            rqsts = []
            for rqst in self.rqst_manager.pops(exist_hosts_d, 10): 
                rqsts.append(rqst)
                if len(rqsts) >= 10: # 避免同一包太大
                    self.logger.debug('send task_reply %s' % rqsts)
                    _send_task_reply_(self, rqsts)
                    rqsts = []
            if len(rqsts) > 0:
                self.logger.debug('send task_reply %s' % rqsts)
                _send_task_reply_(self, rqsts)
                rqsts = []
        elif msg.type == Message.TASK_SEED: # 接收rqsts
            self.logger.info('receive TASK_SEED message')
            task_seeds = TaskSeeds()
            task_seeds.ParseFromString(msg.body)
            task_seeds_rep = TaskSeedsReply(id=task_seeds.id)
            #self.safe_seed_ids = [] # TODO: 
            #if task_seeds.id not in self.safe_seed_ids:
            #    self.logger.warn('no invalid task_seeds id' % task_seeds.id)
            #    task_seeds_rep.status = "deny"
            #else:
            task_seeds_rep.status = "ok"
            for rqst in task_seeds.tasks:
                self.rqst_manager.add(rqst)
            msg.type = Message.TASK_SEED_REP
            msg.body = task_seeds_rep.SerializeToString()
            msgstr = msg.SerializeToString()
            self.sendLine(msgstr)
        elif msg.type == Message.TASK_STATS:
            self.logger.info('receive TASK_STATS message')
            # TASK_STATS_REP
            task_stats = TaskStats()
            task_stats.ParseFromString(msg.body)
            task_stats_rep = TaskStatsReply(id=task_stats.id, time=task_stats.time)
            # 当前队列情况
            domain_exist_info = self.rqst_manager.exists()
            for domain_ in domain_exist_info.keys():
                host_exist_info = domain_exist_info[domain_]
                if not host_exist_info: continue
                sikvs = []
                for host_ in host_exist_info.keys():
                    sikv = sikv_t(key=host_, value=host_exist_info[host_])
                    sikvs.append(sikv)
                task_stats_rep.host_infos.extend(sikvs)
            msg.type = Message.TASK_STATS_REP
            msg.body = task_stats_rep.SerializeToString()
            msgstr = msg.SerializeToString()
            print 'aaaaaaaaaaaaaaaaaaaaaaaaa'
            self.sendLine(msgstr)
        else:
            self.logger.error('invalid message.type=%s' % msg.type)
            msg.type = Message.TASK_SEED_REP
            msg.body = 'invalid msg.type=%s' % msg.type
            msgstr = msg.SerializeToString()
            self.sendLine(msgstr)
    
def main():
    f = Factory()
    f.protocol = MasterServer
    port = g_config.getint('master', 'port')
    print >>sys.stderr, 'listenTCP %s' % port
    reactor.listenTCP(port, f)
    reactor.run()

if __name__ == '__main__':

    if len(sys.argv) != 2:
        print 'Usage: %s <config>' % sys.argv[0]
        sys.exit(1)

    # 读取配置文件
    config_file = sys.argv[1]
    g_config = ConfigParser.ConfigParser()
    g_config.read(config_file)

    main()

