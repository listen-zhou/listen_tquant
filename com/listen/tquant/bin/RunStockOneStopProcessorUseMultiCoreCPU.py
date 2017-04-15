# coding: utf-8
import sys

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
log_name = '\\list_tquant_stock_one_step_business_{batch_num}.log'
when = 'M'
interval = 1
backupCount = 10
level = logging.INFO

def test(msg):
    i = 0
    while i <= 1000000:
        print(msg, '-', str(i))
        i += 1

def init(service):
    service.processing_single_security_code()

if __name__ == "__main__":
    cpu_count = multiprocessing.cpu_count()
    loop_size = 1
    dbService = DbService()
    while True:
        worth_buying_codes = dbService.get_worth_buying_stock()
        size = len(worth_buying_codes)
        print('size', size, 'cpu_count', cpu_count)
        pool = multiprocessing.Pool(processes=cpu_count)
        security_codes = [worth_buying_codes[i][0] for  i in range(len(worth_buying_codes))]
        for security_code in security_codes:
            try:
                result = pool.apply_async(StockOneStopProcessor.processing_single_security_code, args=(security_code, ))
            except Exception:
                print(sys.exc_info())
        # for i in range(100):
        #     pool.apply_async(test, args=('msg-' + str(i), ))
        pool.close()
        pool.join()
        print('loop_size', loop_size)
        loop_size += 1
        time.sleep(30)