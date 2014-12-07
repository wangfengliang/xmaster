#coding=utf-8

import time
import json

from URI import URI
from SpiderSpeed import SpiderSpeed
from Queues import ReadyQueue, TimedoutQueue

# 抓取任务管理
class HostContainer(object):
    def __init__(self, hostname_, mem_max=0, use_cache=False, logger=None): # logger提供了log方法
        self.hostname_ = hostname_
        self.mem_max = mem_max
        self.use_cache = use_cache
        self.logger = logger

        self.is_inprocess = False
        self.mem_queue = []     # 此host下内存中的url列表
        self.cache = []         # 对应的cache(如何实现待定)

    def size(self):
        return len(self.mem_queue)
    
    def add(self, rqst):
        ''' 

           @return: True: 成功且为此host下第一个url[两种情况: a.第一次添加 b.当前队列为空]
                    None: 成功 
                    False:失败
        '''
        if not rqst:
            return False
        if self.use_cache and len(self.mem_queue) > self.mem_max:
            assert False, 'TODO: cache to persist'
            #self.cache.store(rqst) # TODO: cache to persist
            if self.logger:
                self.logger.error('cache to persist not supported!')
            return False
        else:
            self.mem_queue.append(rqst)
            if self.logger:
                self.logger.info('add request %s %s' % (rqst, len(self.mem_queue)))
            if len(self.mem_queue) == 1 and self.is_inprocess == False:  # 队列为空且当前没有抓取过程中
                return True
            return None

    def pop(self):
        if self.mem_queue: 
            self.is_inprocess = True        # 当前host有任务在抓取
            rqst = self.mem_queue.pop(0)
            if self.logger:
                self.logger.debug('HostContainer::pop %s len=%s' % (rqst, len(self.mem_queue)))
            return rqst
        self.is_inprocess = False
        return None

    def pop_all(self):
        for rqst in self.mem_queue:
            yield rqst

    def __str__(self):
        d = {
            'host': self.hostname_,
            'tasks': self.mem_queue,
        }
        return json.dumps(d, indent=4)

    def _print_(self):
        print 'host:', self.hostname_
        print self.mem_queue
        print 

class DomainContainer(object):
    def __init__(self, domain_, logger):
        self.domain_ = domain_
        self.logger = logger
        self.host_lst = []      # 当前域名下存在的host列表  [ host1, host2, ... ]
        self.host_dict = {}     # 每个host对应的url列表     { host1: HostContainer, host2: HostContainer, ... ]

    def add(self, url_hostname, rqst):
        ''' 

           @return: True: 成功且为此host下第一个url 
                    None: 成功 
                    False:失败
        '''
        if not rqst or not url_hostname:
            return False
        if url_hostname not in self.host_dict:
            self.host_lst.append(url_hostname)
            self.host_dict[url_hostname] = HostContainer(url_hostname, logger=self.logger)
        return self.host_dict[url_hostname].add(rqst)

    def pop(self, url_hostname):
        if url_hostname and url_hostname in self.host_dict:
            return self.host_dict[url_hostname].pop()
        else:
            if self.logger:
                self.logger.info('DomainContainer::pop no rqst exist %s' % url_hostname)
            return None

    def pop_all(self):
        for hostname in self.host_dict.keys():
            for rqst in self.host_dict[hostname].pop_all():
                yield rqst

    def exists(self):
        d = {}
        for host_ in self.host_dict.keys():
            exist_num = self.host_dict[host_].size()
            if exist_num <= 0: continue # 空的认为是没有
            d[host_] = exist_num
        return d

    def __str__(self):
        d = {
            'domain': self.domain_,
            'host_lst': self.host_lst,
        }
        for hostname in self.host_lst:
            d[hostname] = str(self.host_dict[hostname])
        return json.dumps(d, indent=4)
        
    def _print_(self):
        print 'domain:', self.domain_
        for hostname in self.host_lst:
            self.host_dict[hostname]._print_()

    def pops(self, exist_hosts_d, rqst_per_host=10):
        for host_ in self.host_dict.keys():
            n = rqst_per_host
            if host_ in exist_hosts_d:
                n -= exist_hosts_d[host_]
                if n <= 0: continue
            for i in range(n):
                rqst = self.host_dict[host_].pop()
                if not rqst: raise StopIteration
                yield rqst

class UrlTaskContainer(object):
    def __init__(self, speed_file, default_speed=5, logger=None):
        self.logger = logger

        self.domain_lst = []  # 当前存在的domain列表 
        self.domain_dict = {} # 每个domain下对应的host列表对象

        self.wait_queue = TimedoutQueue()
        self.ready_queue = ReadyQueue()

        self.speed_file = speed_file
        self.default_speed = default_speed
        self.spider_speed = SpiderSpeed(speed_file=speed_file, default_speed=default_speed)

    ## interface
    def add(self, rqst, url=None):
        ''' 

           @return: True: 成功且为此host下第一个url 
                    None: 成功 
                    False:失败
        '''
        if not rqst:
            return False
        if not url:
            d = json.loads(rqst)
            url = d['url'] if 'url' in d else None
        if not url:
            if self.logger:
                self.logger.error('invalid task no url: %s' % rqst)
            return
        url_hostname = URI.hostname(url)
        url_domain = URI.domain(url_hostname)

        if url_domain not in self.domain_dict:
            self.domain_lst.append(url_domain)
            self.domain_dict[url_domain] = DomainContainer(url_domain, logger=self.logger)
        r = self.domain_dict[url_domain].add(url_hostname, rqst)
        if r == True: # 第一个，直接添加的ready_queue
            rqst2 = self.domain_dict[url_domain].pop(url_hostname)
            if rqst2:
                if self.logger:
                    self.logger.info('add first url into ready_queue %s' % url)
                self._add_to_ready_queue_(rqst2)

    ## interface
    def schedule(self, url):
        ''' 根据之前抓取的url，调度相同host下的url进入wait状态 '''
        if not url:
            return
        url_hostname = URI.hostname(url)
        url_domain = URI.domain(url_hostname)
        if url_domain not in self.domain_dict:
            if self.logger:
                self.logger.info('domain:%s not exist' % url_domain)
            return
        speed = self.spider_speed.speed(url)  # 获得配置速度
        if speed is None or speed < 0:
            speed = 1
        rqst = self.domain_dict[url_domain].pop(url_hostname)
        if not rqst:
            self.logger.info('%s has no more rqst' % url_hostname)
            return 
        if self.logger:
            self.logger.info('schedule into wait_queue %s speed=%s' % (rqst, speed))
        self.wait_queue.enqueue(rqst, speed)  # 放入等待队列

    def pop_all(self):
        self.domain_dict = {} # 每个domain下对应的host列表对象
        for rqst in self.ready_queue.pop_all():
            yield rqst
        for rqst in self.wait_queue.pop_all():
            yield rqst
        for domain_ in self.domain_dict.keys():
            for rqst in self.domain_dict[domain_].pop_all():
                yield rqst

    def exists(self):
        d = {}
        for domain_ in self.domain_dict.keys():
            d2 = self.domain_dict[domain_].exists()
            if not d2: continue
            d[domain_] = d2
        return d
    ## interface
    def pop(self):
        return self._pop_ready_request_()

    ## interface       
    def pop_all_ready(self):
        ''' 所有就绪任务 '''
        return self.ready_queue.pop_all()
    
    def _add_to_ready_queue_(self, rqst):
        self.ready_queue.push(rqst)

    def _pop_ready_request_(self):
        return self.ready_queue.pop() 

    def _check_wait_timeout_(self, max=0):
        ''' 检测timeout超时rqst，放入就绪列表 '''
        cur_time = time.time()
        timedout_items = self.wait_queue.pop_timeout(cur_time, max)
        for rqst in timedout_items:
            if self.logger:
                self.logger.info('transfer wait_queue -> ready_queue %s' % rqst)
            self._add_to_ready_queue_(rqst)

    def __str__(self):
        d = {
            'domain_lst': self.domain_lst,
        }
        for domain_ in self.domain_lst:
            d[domain_] = str(self.domain_dict[domain_])
        #return json.dumps(d)
        return json.dumps(d, indent=4)

    def _print_(self):
        for domain_ in self.domain_lst:
            self.domain_dict[domain_]._print_()

    def pops(self, exist_hosts_d, rqst_per_host=10):
        for domain_ in self.domain_dict.keys():
            for rqst in self.domain_dict[domain_].pops(exist_hosts_d, rqst_per_host):
                yield rqst




