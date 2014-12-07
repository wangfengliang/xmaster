#coding=utf-8

import sys
import time

DEBUG = 1
INFO = 5
WARN = 10
ERROR = 20
 

class MyLogger(object):

    def __init__(self, logger=None, level=DEBUG):
        self.logger = logger
        self.level = level

    def debug(self, msg):
        if self.level > DEBUG:
            return
        if self.logger:
            self.logger.debug(msg)
        else:
            ts = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            print >>sys.stderr, '[%s] DEBUG %s' % (ts, msg)
    def info(self, msg):
        if self.level > INFO:
            return
        if self.logger:
            self.logger.info(msg)
        else:
            ts = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            print >>sys.stderr, '[%s] INFO %s' % (ts, msg)
    def warn(self, msg):
        if self.level > WARN:
            return
        if self.logger:
            self.logger.warn(msg)
        else:
            ts = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            print >>sys.stderr, '[%s] WARN %s' % (ts, msg)
    def error(self, msg):
        if self.level > ERROR:
            return
        if self.logger:
            self.logger.error(msg)
        else:
            ts = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            print >>sys.stderr, '[%s] ERROR %s' % (ts, msg)


