#!/usr/bin/env python
#coding=utf-8

# Copyright (c) Twisted Matrix Laboratories.
# See LICENSE for details.

from twisted.internet.protocol import Protocol, Factory
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor

import json

from message_pb2 import *
from statscol import StatsCollector, MStatsCollector
from list_all_members import list_all_members, list_pb_all_numbers

from UrlTaskContainer import UrlTaskContainer
from mylogger import MyLogger

class SpiderInfos(object):
    def __init__(self):
        self.stats = MStatsCollector()

    def update(self, ping):
        spider_id = ping.id
        for key, value in list_pb_all_numbers(ping):
            self.stats.set_value(spider_id, key, value)
        self.stats._print(spider_id)

class EchoServer(LineReceiver):
    def __init__(self):
        self.current_index = 0
        self.spider_status = SpiderInfos()
        #logger = MyLogger()
        logger = None
        self.rqst_manager = UrlTaskContainer('speed.conf', 5, logger)

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
            print 'bbbbbbbbbb', exist_hosts_d

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
                if len(rqsts) >= 100: # 避免同一包太大
                    print 'aaaaaaaaa', exist_hosts_d, rqsts
                    _send_task_reply_(self, rqsts)
                    rqsts = []
            if len(rqsts) > 0:
                print 'cccccc', rqsts
                _send_task_reply_(self, rqsts)
                rqsts = []
        else:
            print 'error'
    
def main():
    f = Factory()
    f.protocol = EchoServer
    reactor.listenTCP(8000, f)
    reactor.run()

if __name__ == '__main__':
    main()

