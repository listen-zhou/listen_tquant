# coding: utf-8
import threading

import datetime

from com.listen.tquant.redis.StockAverageLineRedisService import StockAverageLineRedisService
from com.listen.tquant.service.stock.StockOneStepBusinessService import StockOneStepBusinessService

from com.listen.tquant.dbservice.Service import DbService
from com.listen.tquant.log.Logger import Logger


import logging

"""
股票行情一条龙服务：
1.股票日K历史行情入库
2.股票日K涨跌幅计算，入库
3.股票均线计算，入库
4.股票均线涨跌幅平均计算，入库
5.done
"""

log_path = 'd:\\python_log\\one_step'
log_name = '\\list_tquant_stock_one_step_business.log'
when = 'H'
interval = 1
backupCount = 10
level = logging.INFO
sleep_seconds = 120
one_time = True

batch_num = 0
processing_log_list = None

dbService = DbService()
logger = Logger(level, log_path, log_name, when, interval, backupCount)
redisService = StockAverageLineRedisService()
mas = [3, 5, 10]
tuple_security_codes = [('002460', 'SZ')]
print('tuple_security_codes len', len(tuple_security_codes), 'batch_num', batch_num)
service = StockOneStepBusinessService(dbService, logger, mas, tuple_security_codes, redisService, False)
service.processing()