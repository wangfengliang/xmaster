#!/usr/bin/env python
#coding=utf-8

# Copyright (c) Twisted Matrix Laboratories.
# See LICENSE for details.

import time
import json, zlib
import logging
import ConfigParser

from twisted.internet.protocol import Protocol, Factory, connectionDone
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
        spider_id = ping.spider_id
        for sfkv in ping.status:
            self.stats.set_value(spider_id, sfkv.key, sfkv.value)
        self.stats._print(spider_id)

class MasterServer(LineReceiver):
    def __init__(self):
        global g_master_name, g_redis_addr, g_redis_port, g_logger
        self.master_name = g_master_name
        self.redis_addr = g_redis_addr
        self.redis_port = g_redis_port
        self.logger = g_logger
        self.spider_status = SpiderInfos()
        self.redis_instance = redis.Redis(host=self.redis_addr, port=self.redis_port, db=0)
        self.rqst_manager = UrlTaskContainer(self.master_name, use_cache=True, redis_instance=self.redis_instance, logger=self.logger)

    def connectionLost(self, reason=connectionDone):
        self.logger.info('connectionLost')

    def connectionMade(self):
        self.logger.info('connectionMade')

    def _send_task_reply_(self, cur_time, rqsts):
        task_rep = TaskReply()
        task_rep.tasks.extend(rqsts)
        msg_ = Message(ts=cur_time, type=Message.REP_TASK)
        msg_.body = zlib.compress(task_rep.SerializeToString())
        msgstr = msg_.SerializeToString()
        self.logger.debug('send REP_TASK %s rqsts, %s bytes' % (len(rqsts), len(msgstr)))
        self.sendLine(msgstr)

    def lineReceived(self, line):

        cur_time = long(time.time())

        # 解析协议
        msg = Message()
        msg.ParseFromString(line)
        if msg.type == Message.PING: # spider and selector
            ping = Ping()
            ping.ParseFromString(zlib.decompress(msg.body))

            self.spider_status.update(ping) # 记录每个节点的状态, TODO: redis存储

            pong = Pong()
            msg.type = Message.PONG
            msgstr = msg.SerializeToString()
            self.logger.debug('send PONG %s bytes' % len(msgstr))
            self.sendLine(msgstr)
        elif msg.type == Message.REQ_TASK: # spider
            task_req = TaskRequest()
            task_req.ParseFromString(zlib.decompress(msg.body))
            #hosts_need_d = {}
            #for host_ in task_req.hosts_need:
            #    hosts_need_d[host_.key] = host_.value
            #self.logger.info('spider request hosts: %s' % hosts_need_d)

            #host_exist_info = self.rqst_manager.exists()
            #self.logger.info('master cached hosts: %s' % host_exist_info)

            hosts_exist_d = {}
            for host_ in task_req.hosts_exist:
                hosts_exist_d[host_.key] = host_.value

            rqsts = []
            #for rqst in self.rqst_manager.pops(hosts_need_d, 20): 
            for rqst in self.rqst_manager.pops(hosts_exist_d, 20): 
                rqsts.append(rqst)
                if len(rqsts) >= 100: # 避免同一包太大
                    self._send_task_reply_(cur_time, rqsts)
                    rqsts = []
            if len(rqsts) > 0:
                self._send_task_reply_(cur_time, rqsts)
                rqsts = []
        elif msg.type == Message.TASK_SEED: # selector
            self.logger.info('receive TASK_SEED message %s' % (cur_time-msg.ts))
            task_seeds = TaskSeeds()
            task_seeds.ParseFromString(zlib.decompress(msg.body))
            for rqst in task_seeds.tasks:
                self.rqst_manager.add(rqst)
        elif msg.type == Message.TASK_STATS: # selector
            self.logger.info('receive TASK_STATS message %s' % (cur_time-msg.ts))
            task_stats_rep = TaskStatsReply()
            # 当前队列情况
            host_exist_info = self.rqst_manager.exists()
            self.logger.info('master cached hosts: %s' % host_exist_info)
            sikvs = []
            for host_ in host_exist_info.keys():
                sikv = sikv_t(key=host_, value=host_exist_info[host_])
                sikvs.append(sikv)
            task_stats_rep.host_infos.extend(sikvs)
            msg_rep = Message(type=Message.TASK_STATS_REP, ts=cur_time)
            msg_rep.body = zlib.compress(task_stats_rep.SerializeToString())
            msgstr = msg_rep.SerializeToString()
            self.logger.debug('send TASK_STATS_REP %s bytes' % len(msgstr))
            self.sendLine(msgstr)
        else:
            self.logger.error('invalid message type=%s' % (msg.type))
    
def main():
    f = Factory()
    f.protocol = MasterServer
    port = g_config.getint('master', 'port')
    g_logger.info('listenTCP %s' % port)
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

    g_master_name = g_config.get('master', 'name')
    level = g_config.get('master', 'level') if g_config.has_option('master', 'level') else "DEBUG"
    debug = g_config.getboolean('master', 'debug') if g_config.has_option('master', 'debug') else True
    logfile = g_config.get('master', 'logfile') if g_config.has_option('master', 'logfile') else None
    logname = g_config.get('master', 'logname') if g_config.has_option('master', 'logname') else None
    if not debug:
        assert logfile, 'logfile must be set when not debug mode'
    g_logger = Logger.getLogger(logname, logfile, level=level, debug=debug)
    g_redis_addr = g_config.get('master', 'redis_addr') if g_config.has_option('master', 'redis_addr') else 'localhost'
    g_redis_port = g_config.getint('master', 'redis_port') if g_config.has_option('master', 'redis_port') else 6379

    main()

