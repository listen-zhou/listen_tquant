# coding: utf-8
import configparser
import os

import logging
import redis
import traceback


class RedisService(object):
    def __init__(self):
        config = configparser.ConfigParser()
        os.chdir('../config')
        config.read('redis.cfg')
        redis_section = config['redis']
        if redis_section:
            host = redis_section['redis.host']
            port = int(redis_section['redis.port'])
            db = redis_section['redis.db']
            self.queue = redis_section['redis.block.average.queue']
            pool = redis.ConnectionPool(host=host, port=port, db=db)
            self.r = redis.Redis(connection_pool=pool)
        else:
            print('error')
            raise FileNotFoundError('redis.cfg redis section not found!!!')
        print('RedisService init...')

    def put_log(self, log_list, level=logging.INFO):
        self.r.lpush(self.queue, log_list)

    def get_log(self):
        print('redis service get log init...')
        while True:
            try:
                msg = self.r.blpop(self.queue, 0)
                msg = msg[1].decode('utf-8')
                print('redis get log', msg)
            except Exception:
                print(traceback.format_exc())