#!/usr/bin/env python
#coding=utf-8

from URI import URI

class SpiderSpeed(object):
    def __init__(self, speed_file=None, speeds={}, default_speed=10, logger=None):
        self.default_speed = default_speed
        self.logger = logger
        self.speeds = speeds

        self.speed_file = speed_file
        if self.speed_file:
            self._load_(self.speed_file)
        
    def _load_(self, speed_file):
        if self.speed_file is None:
            return
        with open(self.speed_file, 'r') as fd:
            for line in fd:
                line = line.strip()
                if len(line) <= 0:
                    continue
                elif line.startswith('#'):
                    if self.logger: self.logger.debug('SpiderSpeed skip %s' % line)
                    continue
                host, speed = line.split()
                if host in self.speeds:
                    if self.logger: self.logger.warn('SpiderSpeed set %s more than once' % host)
                self.speeds[host] = float(speed)

    def speed(self, url):
        ret = self.default_speed
        _domains = URI.domains(url)
        for _domain in _domains:
            if _domain in self.speeds:
                return self.speeds[_domain]
        return ret
 
if __name__ == "__main__":

    speeds = {'www.baidu.com': 10, 'baidu.com': 5, 'www.hichao.com': 12}
    #spider_speed = SpiderSpeed(speeds=speeds, default_speed=200)
    spider_speed = SpiderSpeed(speed_file='speed.conf', default_speed=200)
    
    print spider_speed.speeds
    urls = ["http://www.baidu.com/", "http://baidu.com/", "http://hichao.com/"]
    for url in urls:
        print url, spider_speed.speed(url)

