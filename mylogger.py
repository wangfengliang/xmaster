#coding=utf-8

import sys
import time

import time
import logging
from logging.handlers import TimedRotatingFileHandler

level_d = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARN": logging.WARN,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "FATAL": logging.FATAL,
}

class Logger(object):

    @staticmethod
    def getLogger(logger_name, log_file, level='DEBUG', debug=True):
        logger_ = logging.getLogger(logger_name)
        logger_.setLevel(level_d.get(level, logging.DEBUG))
        formater = logging.Formatter('[%(asctime)s] %(name)s %(filename)s +%(lineno)d %(levelname)s %(message)s')
        handler = None
        if debug:
            handler = logging.StreamHandler()
        else:
            handler = TimedRotatingFileHandler(log_file, 'h', 1, 24*10)
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(formater)
        logger_.addHandler(handler)
        return logger_

if __name__ == "__main__":
    logger_debug = Logger.getLogger('spider', None, level='DEBUG', debug=True) # <==> logger = Logger.getLogger('spider') # debug
    logger_online = Logger.getLogger('spider', 'spider-<pid>.log', level='INFO', debug=False) # <==> logger = Logger.getLogger('spider') # debug

    #logger = logger_debug
    logger = logger_online

    while True:
        logger.debug('debug message')
        logger.info('info message')
        logger.warn('warn message')
        logger.error('error message')
        logger.critical('critical message')
    
        #logger.log(logging.INFO, "We have a %s", "mysterious problem", exc_info=1)
        logger.log(logging.INFO, 'We have a %s %s', "mysterious problem", 'bb')
        try:
            raise ValueError('value Error')
        except Exception, e:
            logger.info('%s %s!!!', 'aaa', e, exc_info=1)
    
        time.sleep(0.01)
    
