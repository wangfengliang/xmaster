#!/usr/bin/env python
#coding=utf-8

# Copyright (c) Twisted Matrix Laboratories.
# See LICENSE for details.

import json

from twisted.internet.protocol import Protocol, Factory
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor

from message_pb2 import *
from statscol import StatsCollector, MStatsCollector
from list_all_members import list_all_members, list_pb_all_numbers

from UrlTaskContainer import UrlTaskContainer
from mylogger import MyLogger

import redis

class SpiderInfos(object):
    def __init__(self):
        self.stats = MStatsCollector()

    def update(self, ping):
        spider_id = ping.id
        for key, value in list_pb_all_numbers(ping):
            self.stats.set_value(spider_id, key, value)
        self.stats._print(spider_id)

class EchoServer(LineReceiver):
    def __init__(self, logger=None):
        self.current_index = 0
        self.logger = logger
        self.spider_status = SpiderInfos()
        self.master_name = 'xmaster'
        self.redis_instance = redis.Redis(host='localhost', port=6379, db=0)
        self.rqst_manager = UrlTaskContainer(self.master_name, use_cache=True, redis_instance=self.redis_instance, logger=self.logger)

    def connectionMade(self):
        print 'connectionMade'
        self.urls = []
        with open('tasks.json') as fd:
            for line in fd.readlines():
                rqst = line.strip()
                self.urls.append(rqst)
                self.rqst_manager.add(rqst)
        self.current_index = 0

    def lineReceived(self, line):
        #print 'dataReceived', line
 
        if self.current_index >= len(self.urls):
            print 'no urls'
            #return 
            self.current_index = 0

        # 解析
        msg = Message()
        msg.ParseFromString(line)
        print 'msg.type=', msg.type
        if msg.type == Message.PING:
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
            task_req = TaskRequest()
            task_req.ParseFromString(msg.body)
            exist_hosts_d = {}
            #for domain_ in task_req.host_infos:
            #    for host_ in domain_.values:
            #        exist_hosts_d[host_.key] = host_.value
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

            print 'aaaaaaaaa', exist_hosts_d 
            print '222222222', self.rqst_manager.exists()
            rqsts = []
            for rqst in self.rqst_manager.pops(exist_hosts_d, 10): 
                rqsts.append(rqst)
                if len(rqsts) >= 100: # 避免同一包太大
                    print 'bbbbbbbbbbbb', rqsts
                    _send_task_reply_(self, rqsts)
                    rqsts = []
            if len(rqsts) > 0:
                print 'bbbbbbbbbbbb', rqsts
                _send_task_reply_(self, rqsts)
                rqsts = []
        elif msg.type == Message.TASK_SEED: # 接收rqsts
            task_seeds = TaskSeeds()
            task_seeds.ParseFromString(msg.body)
            task_seeds_rep = TaskSeedsReply(id=task_seeds.id)
            if task_seeds.id not in self.safe_seed_ids:
                print 'no invalid task generate'
                task_seeds_rep.status = "deny"
            else:
                task_seeds_rep.status = "ok"
                for rqst in task_seeds.tasks:
                    self.rqst_manager.add(rqst)
            msg.type = Message.TASK_SEED_REP
            msg.body = task_seeds_rep.SerializeToString()
            msgstr = msg.SerializeToString()
            self.sendLine(msgstr)
        else:
            print 'error message.type=%s' % msg.type
            msg.type = Message.TASK_SEED_REP
            msg.body = 'invalid msg.type=%s' % msg.type
            msgstr = msg.SerializeToString()
            self.sendLine(msgstr)
    
def main():
    f = Factory()
    f.protocol = EchoServer
    reactor.listenTCP(8000, f)
    reactor.run()

if __name__ == '__main__':
    main()

