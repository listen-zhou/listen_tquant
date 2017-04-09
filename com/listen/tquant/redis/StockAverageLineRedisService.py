# coding utf-8
import configparser
import os
import redis
import time

# https://github.com/ServiceStack/redis-windows
class StockAverageLineRedisService():
    def __init__(self):
        print('init ..')
        config = configparser.ConfigParser()
        os.chdir('../config')
        config.read('redis.cfg')
        redis_section = config['redis']
        if redis_section:
            host = redis_section['redis.host']
            port = int(redis_section['redis.port'])
            db = redis_section['redis.db']
            self.average_line_queue = redis_section['redis.block.average.queue']
            self.pool = redis.ConnectionPool(host=host, port=port, db=db)
            self.r = redis.Redis(connection_pool=self.pool)
        else:
            raise FileNotFoundError('redis.cfg redis section not found!!!')

    def product_average_line_message(self, security_code, exchange_code):
        if self.average_line_queue is not None \
                and len(self.average_line_queue) > 0:
            print('redis send ', security_code, exchange_code)
            self.r.rpush(self.average_line_queue, str(security_code) + ',' + str(exchange_code))