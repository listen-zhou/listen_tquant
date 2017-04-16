# coding: utf-8
import random
import time
import os

from com.listen.tquant.redis.RedisService import RedisService
import sys
import traceback

class Business(object):
    redisService = RedisService()

    def __init__(self, i):
        self.i = i

        # self.redisService = redisService
        # self.get_log = get_log
        # self.put_log = put_log
        # self.r = r
        # self.queue = queue
        print('Business init...')

    def bus(self, pid):
        print('bus init pid ', pid, '...')
        i = 0
        while i <= 20:
            kk = pid + '-' + str(i)
            # self.r.lpush(self.queue, kk)
            self.redisService.put_log(kk)
            print('bus lpush', kk)
            time.sleep(random.random())
            i += 1

    def consume(self):
        print('consume init ...')
        self.redisService.get_log()


    def long_time_task(self, i):
        print('Run task %s (%s)...' % (i, os.getpid()))
        time.sleep(random.random() * 3)
        print(i)