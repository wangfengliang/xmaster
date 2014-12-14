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
            redis_host_zkey = "spider:%s:hosts:%s" % (self.master_name, self.hostname_)
            sz = self.redis_instance.zcard(redis_host_zkey)
            #self.logger.debug('cache %s %s' % (self.hostname_, sz))
            return sz
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
            if ret == 1:
                self.logger.debug('cache add %s %s' % (self.hostname_, rqst))
            else:
                self.logger.debug('cache add failed %s %s' % (self.hostname_, rqst))
    
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
            self.logger.debug('cache %s pop %s' % (self.hostname_, rqst))
            return rqst
        else:
            rqst = self.mem_queue.pop(0)
            if self.logger:
                self.logger.debug('HostContainer::pop %s len=%s' % (rqst, len(self.mem_queue)))
            return rqst
        return None

class UrlTaskContainer(object):
    def __init__(self, master_name, use_cache=True, redis_instance=None, logger=None):
        self.master_name = master_name
        self.logger = logger
        self.use_cache = use_cache
        self.redis_instance = redis_instance

        self.host_dict = {}
        self.rsync_host_with_cache()

    def rsync_host_with_cache(self):
        if self.redis_instance:
            redis_keys_pattern = "spider:%s:hosts:*" % self.master_name
            host_keys_exist = self.redis_instance.keys(redis_keys_pattern)
            for host_key in host_keys_exist:
                host_ = host_key.split(':')[-1]
                if host_ not in self.host_dict:
                    self.host_dict[host_] = HostContainer(self.master_name, host_, use_cache=self.use_cache, redis_instance=self.redis_instance, logger=self.logger)
            
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

        if url_hostname not in self.host_dict:
            self.host_dict[url_hostname] = HostContainer(self.master_name, url_hostname, use_cache=self.use_cache, redis_instance=self.redis_instance, logger=self.logger)
        return self.host_dict[url_hostname].add(rqst)

#    def pops(self, hosts_need_d, rqst_per_host=10):
#        for host_ in self.host_dict.keys():
#            n = rqst_per_host
#            if host_ in hosts_need_d:
#                n = hosts_need_d[host_]
#            for i in range(n):
#                rqst = self.host_dict[host_].pop()
#                if not rqst: break
#                yield rqst
#

    def pops(self, hosts_exist_d, rqst_per_host=10):
        for host_ in self.host_dict.keys():
            n = rqst_per_host
            if host_ in hosts_exist_d:
                n -= hosts_exist_d[host_]
            if n <= 0: continue
            for i in range(n):
                rqst = self.host_dict[host_].pop()
                if not rqst: break
                yield rqst

    def exists(self):
        d = {}
        for host_ in self.host_dict.keys():
            n = self.host_dict[host_].size()
            d[host_] = n
        return d

