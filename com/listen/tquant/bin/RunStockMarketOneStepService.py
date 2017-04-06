# coding: utf-8
import threading

import datetime

from com.listen.tquant.service.stock.StockAverageLineChgAvgService import StockAverageLineChgAvgService
from com.listen.tquant.service.stock.StockAverageLineService import StockAverageLineService
from com.listen.tquant.service.stock.StockDayKlineChangePercentService import StockDayKlineChangePercentService
from com.listen.tquant.service.stock.StockDayKlineService import StockDayKlineService


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

log_path = 'd:\\python_log\\day_kline'
log_name = '\\list_tquant_stock_market_one_step.log'
when = 'H'
interval = 1
backupCount = 10
level = logging.INFO
sleep_seconds = 120
one_time = True

batch_num = 6
processing_log_list = None

dbService = DbService()

mas = [3, 5, 10]
tuple_security_codes = [('002460', 'SZ'), ('002466', 'SZ')]
print('tuple_security_codes len', len(tuple_security_codes), 'batch_num', batch_num)
if tuple_security_codes is not None and len(tuple_security_codes) > 0:
    batch_name = 'batch-' + str(batch_num)
    logger = Logger(level, log_path, log_name, when, interval, backupCount)
    stockDayKlineServiceBatch = StockDayKlineService(dbService, logger, sleep_seconds, one_time)
    stockDayKlineChangePercentService = StockDayKlineChangePercentService(dbService, logger, sleep_seconds, one_time)
    for item in tuple_security_codes:
        security_code = item[0]
        exchange_code = item[1]
        stockDayKlineServiceBatch.processing_single_security_code(None, security_code, exchange_code, 0)
        stockDayKlineChangePercentService.processing_single_security_code(None, security_code, exchange_code, None)
        for ma in mas:
            stockAverageLineService = StockAverageLineService(dbService, ma, logger, sleep_seconds, one_time)
            stockAverageLineService.processing_single_security_code(None, security_code, exchange_code, None)
            stockAverageLineChgAvgService = StockAverageLineChgAvgService(dbService, ma, logger, sleep_seconds, one_time)
            stockAverageLineChgAvgService.processing_single_security_code(None, security_code, exchange_code, None)







