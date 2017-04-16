# coding: utf-8
import configparser
import os
import sys

import redis

from com.listen.tquant.service.stock.StockOneStopProcessor import StockOneStopProcessor
from com.listen.tquant.dbservice.Service import DbService
from com.listen.tquant.log.Logger import Logger


import logging

"""
股票行情一条龙服务，利用多核CPU：
1.股票日K历史行情入库
2.股票日K涨跌幅计算，入库
3.股票均线计算，入库
4.股票均线涨跌幅平均计算，入库
5.done
"""

import multiprocessing
import time

log_path = 'd:\\python_log\\one_step'
log_name = '\\list_tquant_stock_one_step_business_all.log'
when = 'M'
interval = 1
backupCount = 10
level = logging.INFO

def test(msg):
    i = 0
    while i <= 1000000:
        print(msg, '-', str(i))
        i += 1

def init(security_code, r, queue):
    StockOneStopProcessor.processing_single_security_code(security_code, r, queue)

if __name__ == "__main__":

    config = configparser.ConfigParser()
    os.chdir('../config')
    config.read('redis.cfg')
    redis_section = config['redis']
    if redis_section:
        host = redis_section['redis.host']
        port = int(redis_section['redis.port'])
        db = redis_section['redis.db']
        queue = redis_section['redis.block.average.queue']
        pool = redis.ConnectionPool(host=host, port=port, db=db)
        r = redis.Redis(connection_pool=pool)
    else:
        raise FileNotFoundError('redis.cfg redis section not found!!!')

    cpu_count = multiprocessing.cpu_count()
    loop_size = 1
    dbService = DbService()
    logger = Logger(level, log_path, log_name, when, interval, backupCount)
    while True:
        worth_buying_codes = dbService.get_worth_buying_stock()
        size = len(worth_buying_codes)
        print('size', size, 'cpu_count', cpu_count)
        pool = multiprocessing.Pool(processes=cpu_count)
        security_codes = [worth_buying_codes[i][0] for  i in range(len(worth_buying_codes))]
        for security_code in security_codes:
            try:
                result = pool.apply_async(init, args=(security_code, r, queue))
            except Exception:
                sys.exc_traceback()
        pool.close()
        pool.join()
        print('loop_size', loop_size)
        loop_size += 1
        time.sleep(30)