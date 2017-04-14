# coding: utf-8
import sys

from com.listen.tquant.service.stock.StockOneStepBusinessService import StockOneStepBusinessService
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

# sleep_seconds = 120
# one_time = True

# batch_num = 0
# processing_log_list = None
#
#
# logger = Logger(level, log_path, log_name, when, interval, backupCount)
# mas = [3, 5, 10]
# tuple_security_codes = [('002466', 'SZ')]
# print('tuple_security_codes len', len(tuple_security_codes), 'batch_num', batch_num)
# service = StockOneStepBusinessService(logger, mas, tuple_security_codes, False)
# service.processing()

import multiprocessing
import time

log_path = 'd:\\python_log\\one_step'
log_name = '\\list_tquant_stock_one_step_business_{batch_num}.log'
when = 'H'
interval = 1
backupCount = 10
level = logging.INFO

def init(service):
    service.processing()

if __name__ == "__main__":
    cpu_count = multiprocessing.cpu_count()
    loop_size = 1
    dbService = DbService()
    while True:
        list_tuple = []
        worth_buying_codes = dbService.get_worth_buying_stock()
        size = len(worth_buying_codes)
        if size > cpu_count:
            batch_size = size // cpu_count
            process_size = cpu_count
        else:
            batch_size = 1
            process_size = size
        print('size', size, 'cpu_count', cpu_count, 'batch_size', batch_size, 'process_size', process_size)
        i = 0
        pool = multiprocessing.Pool(processes=process_size)
        mas = [3, 5, 10]
        while i < process_size:
            logger = Logger(level, log_path, log_name.format(batch_num=i), when, interval, backupCount)
            # 如果i是最后一批
            if i == process_size - 1:
                tuple_security_codes = worth_buying_codes[i * batch_size : size]
            else:
                tuple_security_codes = worth_buying_codes[i * batch_size: i * batch_size + batch_size]
            print('tuple_security_codes len', len(tuple_security_codes), tuple_security_codes)
            service = StockOneStepBusinessService(logger, mas, tuple_security_codes)
            try:
                result = pool.apply_async(init(service))
            except Exception:
                print(sys.exc_info())
            i += 1
        pool.close()
        pool.join()
        print('loop_size', loop_size)
        loop_size += 1
        time.sleep(30)