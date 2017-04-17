# coding: utf-8
import configparser
import os

import logging

import datetime
import redis
import traceback
from com.listen.tquant.log.Logger import Logger

class RedisService(object):
    log_path = 'd:\\python_log\\one_step'
    log_name = '\\list_tquant_stock_one_step_business_{time}.log'.format(time='ccc')
    when = 'M'
    interval = 1
    backupCount = 10
    level = logging.INFO
    time = datetime.datetime.now()
    logger = Logger(level, log_path, log_name, when, interval, backupCount)

    config = configparser.ConfigParser()
    os.chdir('../config')
    config.read('redis.cfg')
    redis_section = config['redis']
    r = None
    queue = None
    if redis_section:
        host = redis_section['redis.host']
        port = int(redis_section['redis.port'])
        db = redis_section['redis.db']
        queue = redis_section['redis.block.average.queue']
        pool = redis.ConnectionPool(host=host, port=port, db=db)
        r = redis.Redis(connection_pool=pool)
    else:
        print('error')
        raise FileNotFoundError('redis.cfg redis section not found!!!')
    print('RedisService init...')
    def __init__(self):
        pass

    def put_log(self, log_list, level=logging.INFO):
        msg = RedisService.logger.format_list(log_list)
        RedisService.r.lpush(RedisService.queue, msg)

    def get_log(self):
        print('redis service get log init...')
        while True:
            try:
                msg = self.r.blpop(RedisService.queue, 0)
                msg = msg[1].decode('utf-8')
                print('redis get log', msg)
            except Exception:
                print(traceback.format_exc())

    @staticmethod
    def consume_log():
        print('redis service consume init...')
        while True:
            try:
                msg = RedisService.r.blpop(RedisService.queue, 0)
                msg = msg[1].decode('utf-8')
                print('redis get log', msg)
                RedisService.logger.base_log([msg])
            except Exception:
                print(traceback.format_exc())

if __name__ == '__main__':
    RedisService.consume_log()