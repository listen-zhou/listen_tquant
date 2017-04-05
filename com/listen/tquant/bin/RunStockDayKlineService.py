# coding: utf-8
import threading

import datetime

from com.listen.tquant.service.stock.StockDayKlineService import StockDayKlineService


from com.listen.tquant.dbservice.Service import DbService
from com.listen.tquant.log.Logger import Logger


import logging


log_path = 'd:\\python_log\\day_kline'
log_name = '\\list_tquant_day_kline_{0[0]}.log'
when = 'H'
interval = 1
backupCount = 10
level = logging.INFO
sleep_seconds = 120
one_time = True

batch_num = 6
threads = []
processing_log_list = None

dbService = DbService()

# 多线程处理全部日K数据，分组处理
calendar_max_the_date = dbService.get_calendar_max_the_date()
batch_list = dbService.get_batch_list_security_codes(500)

if batch_num < len(batch_list):
    batch_name = 'batch-' + str(batch_num)
    log_name = log_name.format([batch_name])
    print(log_path + log_name)
    logger = Logger(level, log_path, log_name, when, interval, backupCount)
    batchService = StockDayKlineService(DbService(), logger, sleep_seconds, one_time)
    batchServiceThread = threading.Thread(target=batchService.processing_security_codes,
                                          args=(processing_log_list, batch_list[batch_num],
                                                calendar_max_the_date, batch_name
                                                )
                                          )
    batchServiceThread.setName('StockDayKlineService-' + batch_name)
    threads.append(batchServiceThread)

print('batch_list len', len(batch_list), 'batch_num', batch_num)
if len(threads) > 0:
    for thread in threads:
        # 设置为守护线程
        thread.setDaemon(False)
        thread.start()
else:
    print('batch_list len', len(batch_list), 'batch_num', batch_num, 'is to large, exit')
###########################################################################
















# 处理异常日K数据对应的股票代码，股票的开盘价、最高价、最低价、收盘价 <=0 的情况，这是为什么
# 根据指定的sql，查询过滤异常数据的日K数据对应的股票代码
# tuple_security_codes = dbService.get_day_kline_except_security_codes()
# print('tuple_security_codes len', len(tuple_security_codes), 'batch_num', batch_num)
# if tuple_security_codes is not None and len(tuple_security_codes) > 0:
#     batch_name = 'batch-' + str(batch_num)
#     log_name = log_name.format([batch_name])
#     print(log_path + log_name)
#     logger = Logger(level, log_path, log_name, when, interval, backupCount)
#     stockDayKlineServiceBatch = StockDayKlineService(DbService(), logger, sleep_seconds, one_time)
#     for item in tuple_security_codes:
#         security_code = item[0]
#         exchange_code = item[1]
#         stockDayKlineServiceBatch.processing_single_security_code(None, security_code, exchange_code, 0)




