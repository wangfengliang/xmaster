#coding=utf-8

import time
import json

from URI import URI


# 抓取任务管理
class HostContainer(object):
    def __init__(self, master_name, hostname_, use_cache=False, redis_instance=None, logger=None): # logger提供了log方法
        self.master_name = master_name
        self.hostname_ = hostname_
        self.use_cache = use_cache
        self.redis_instance = redis_instance
        self.logger = logger

        self.mem_queue = []     # 此host下内存中的url列表

    def size(self):
        if self.use_cache:
            #redis_hosts_hkey = "spider:%s:hosts" % self.master_name
            #return self.redis_instance.hget(redis_hosts_hkey, self.hostname_) # 此host的数量
            redis_host_zkey = "spider:%s:hosts:%s" % (self.master_name, self.hostname_)
            return self.redis_instance.zcard(redis_host_zkey)
        return len(self.mem_queue)
    
    def add(self, rqst):
        ''' 
           @return: True: 成功且为此host下第一个url[两种情况: a.第一次添加 b.当前队列为空]
                    None: 成功 
                    False:失败
        '''
        if not rqst:
            return False
        if self.use_cache:
            #redis_hosts_hkey = "spider:%s:hosts" % self.master_name
            redis_host_zkey = "spider:%s:hosts:%s" % (self.master_name, self.hostname_)
            ret = self.redis_instance.zadd(redis_host_zkey, rqst, 1.0) # TODO: 判断返回结果
            #if ret == 1: ret2 = self.redis_instance.hincrby(redis_hosts_hkey, self.hostname_, 1) # 增加此host的数量
            print 'HostContainer::zadd', rqst, ret
            return True 
        else:
            self.mem_queue.append(rqst)
            if self.logger:
                self.logger.info('add request %s %s' % (rqst, len(self.mem_queue)))
            return True

    def pop(self):
        if self.use_cache:
            #redis_hosts_hkey = "spider:%s:hosts" % self.master_name
            redis_host_zkey = "spider:%s:hosts:%s" % (self.master_name, self.hostname_)
            rqsts = self.redis_instance.zrange(redis_host_zkey, 0, 0) # TODO: 判断返回结果
            if not rqsts: return None
            rqst = rqsts[0]
            self.redis_instance.zrem(redis_host_zkey, rqst)
            #ret2 = self.redis_instance.hincrby(redis_hosts_hkey, self.hostname_, -1) # 增加此host的数量
            print 'HostContainer::pop', rqst
            return rqst
        else:
            rqst = self.mem_queue.pop(0)
            if self.logger:
                self.logger.debug('HostContainer::pop %s len=%s' % (rqst, len(self.mem_queue)))
            return rqst
        return None

class DomainContainer(object):
    def __init__(self, master_name, domain_, use_cache=False, redis_instance=None, logger=None):
        self.master_name = master_name
        self.domain_ = domain_
        self.use_cache = use_cache
        self.redis_instance = redis_instance
        self.logger = logger
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
            self.host_dict[url_hostname] = HostContainer(self.master_name, url_hostname, use_cache=self.use_cache, redis_instance=self.redis_instance, logger=self.logger)
        return self.host_dict[url_hostname].add(rqst)

    def pop(self, url_hostname):
        if url_hostname and url_hostname in self.host_dict:
            return self.host_dict[url_hostname].pop()
        else:
            if self.logger:
                self.logger.info('DomainContainer::pop no rqst exist %s' % url_hostname)
            return None

    def exists(self):
        d = {}
        for host_ in self.host_dict.keys():
            exist_num = self.host_dict[host_].size()
            #if exist_num <= 0: continue # 空的认为是没有
            d[host_] = exist_num
        return d

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
    def __init__(self, master_name, use_cache=False, redis_instance=None, logger=None):
        self.master_name = master_name
        self.logger = logger
        self.use_cache = use_cache
        self.redis_instance = redis_instance

        self.domain_dict = {} # 每个domain下对应的host列表对象

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
            self.domain_dict[url_domain] = DomainContainer(self.master_name, url_domain, use_cache=self.use_cache, redis_instance=self.redis_instance, logger=self.logger)
        return self.domain_dict[url_domain].add(url_hostname, rqst)

    def pops(self, exist_hosts_d, rqst_per_host=10):
        for domain_ in self.domain_dict.keys():
            for rqst in self.domain_dict[domain_].pops(exist_hosts_d, rqst_per_host):
                yield rqst


    def exists(self):
        d = {}
        for domain_ in self.domain_dict.keys():
            d2 = self.domain_dict[domain_].exists()
            if not d2: continue
            d[domain_] = d2
        return d

