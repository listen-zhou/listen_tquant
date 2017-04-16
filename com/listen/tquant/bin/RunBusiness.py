# coding: utf-8
import configparser
import os

import redis

from com.listen.tquant.redis.RedisService import RedisService
from com.listen.tquant.testpool.Business import Business
import multiprocessing

def init_consume(i, business):
    print('consume', str(i), business)


def init_bus(business, pid):
    business.bus(pid)

def long_time_task_wrapper(cls_instance, i):
    cls_instance.bus(i)

def long_time_task_consume(cls_instance):
    cls_instance.consume()
#
# def put_log(msg):
#     config = configparser.ConfigParser()
#     os.chdir('../config')
#     config.read('redis.cfg')
#     redis_section = config['redis']
#     if redis_section:
#         host = redis_section['redis.host']
#         port = int(redis_section['redis.port'])
#         db = redis_section['redis.db']
#         queue = redis_section['redis.block.average.queue']
#         pool = redis.ConnectionPool(host=host, port=port, db=db)
#         r = redis.Redis(connection_pool=pool)
#     else:
#         print('error')
#         raise FileNotFoundError('redis.cfg redis section not found!!!')
#     r.lpush(queue, msg)
#
# def get_log():
#     config = configparser.ConfigParser()
#     os.chdir('../config')
#     config.read('redis.cfg')
#     redis_section = config['redis']
#     if redis_section:
#         host = redis_section['redis.host']
#         port = int(redis_section['redis.port'])
#         db = redis_section['redis.db']
#         queue = redis_section['redis.block.average.queue']
#         pool = redis.ConnectionPool(host=host, port=port, db=db)
#         r = redis.Redis(connection_pool=pool)
#     else:
#         print('error')
#         raise FileNotFoundError('redis.cfg redis section not found!!!')
#     return r.brpop(queue, 0)

if __name__ == '__main__':
    cpu_count = multiprocessing.cpu_count()
    manager = multiprocessing.Manager()
    pool = multiprocessing.Pool(processes=cpu_count)

    business = Business(1)
    pool.apply_async(long_time_task_consume, args=(business, ))
    for i in range(20):
        pool.apply_async(long_time_task_wrapper, args=(business, 'pid-' + str(i), ))

    pool.close()
    pool.join()
    print('done')