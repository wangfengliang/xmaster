#!/usr/bin/env python
#coding=utf-8

import time

class TimedoutItem(object):
    def __init__(self, data, log_time, timeout):
        self.data = data
        self.log_time = log_time
        self.timeout = timeout

    def is_timeout(self, cur_time):
        if self.log_time + self.timeout <= cur_time+0.001:
            return True
        return False

    def __str__(self):
        return '%s %s %s' % (self.log_time, self.timeout, self.data)

class TimedoutQueue(object):
    def __init__(self, max_items=10000):
        self.max_items = max_items
        #self.queue = []
        self.queue = set()

    def enqueue(self, task, timeout=10):
        item = TimedoutItem(task, time.time(), timeout)
        self.queue.add(item)

    def enqueue2(self, item):
        self.queue.add(item)

    def pop_all(self):
        for item in self.queue:
            yield item.data

    def pop_timeout(self, cur_time, max_item=0):
        if self.isempty():
            return []
        # 使用时需要测试大数据时运行时间
        if max_item > 0:
            items_timeout = [ item for i, item in enumerate(self.queue) if i <= max and item.is_timeout(cur_time) ]
        else:
            items_timeout = [ item for item in self.queue if item.is_timeout(cur_time) ]
        self.queue = set(self.queue).difference(set(items_timeout))
        return [ item.data for item in items_timeout ]

    def size(self):
        return len(self.queue)

    def isempty(self):           
        return self.queue is None or len(self.queue) <= 0

class ReadyQueue(object):
    def __init__(self):
        self.queue = []

    def push(self, data):
        self.queue.append(data)

    def pop(self, offset=0):
        if self.isempty():
            return None
        return self.queue.pop(offset)

    def pop_all(self):
        lst = self.queue[:]
        self.queue = []
        return lst

    def size(self):
        return len(self.queue)

    def isempty(self):           
        return self.queue is None or len(self.queue) <= 0

if __name__ == "__main__":
    import random

    timedout_queue = TimedoutQueue()

    start1 = time.time()

    stats = {}
    for i in xrange(100000):
        t = random.randint(5, 20)
        item = TimedoutItem('aa', time.time(), t)
        timedout_queue.enqueue2(item)
        if t not in stats:
            stats[t] = 0
        stats[t] += 1
    end1 = time.time()

    stats2 = {}
    time.sleep(5)

    start2 = time.time()

    i = 0
    while True:
        t = time.time()
        items = timedout_queue.pop_timeout(t)
        if len(items) > 0:
            i += len(items)
            t2 = time.time()
            print i, len(items), t2-t
        if i >= 100000:
            break
        time.sleep(0.01)
   
    end2 = time.time()

    print 'enqueue=', end1 - start1
    print 'dequeue=', end2 - start2
    print stats
