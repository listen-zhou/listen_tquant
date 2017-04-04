# coding: utf-8
import threading

import datetime

from com.listen.tquant.service.stock.StockAverageLineService import StockAverageLineService
from com.listen.tquant.dbservice.Service import DbService
from com.listen.tquant.log.Logger import Logger
import logging


log_path = 'd:\\python_log\\average'
log_name = '\\list_tquant_average_ma{0[0]}_{0[1]}.log'
when = 'H'
interval = 1
backupCount = 10
level = logging.INFO
sleep_seconds = 120
one_time = True
# mas = [3, 5, 10, 20, 30, 60, 120, 250]
ma = 10
batch_num = 0
dbService = DbService()
processing_log_list = None


# 获取股票代码按batch_size分批后的list
# threads = []
#
#
# batch_list = dbService.get_batch_list_security_codes(500)
# calendar_max_the_date = dbService.get_calendar_max_the_date()
# # 分批处理全量日K均值数据
# if batch_num < len(batch_list):
#     batch_name = 'batch-' + str(batch_num)
#     log_name = log_name.format([ma, batch_name])
#     print(log_path + log_name)
#     logger = Logger(level, log_path, log_name, when, interval, backupCount)
#     batchService = StockAverageLineService(DbService(), ma, logger, sleep_seconds, one_time)
#     batchServiceThread = threading.Thread(target=batchService.processing_security_codes,
#                                           args=(processing_log_list, batch_list[batch_num],
#                                                 calendar_max_the_date, batch_name
#                                                 )
#                                           )
#     batchServiceThread.setName('StockAverageLineService-ma' + str(ma) + '_' + batch_name)
#     threads.append(batchServiceThread)
# print('batch_list len', len(batch_list), 'batch_num', batch_num)
# if len(threads) > 0:
#     for thread in threads:
#         # 设置为守护线程
#         thread.setDaemon(False)
#         thread.start()
# else:
#     print('batch_list len', len(batch_list), 'batch_num', batch_num, 'is to large, exit')
#####################################################################











# 筛选异常股票代码，并处理这些异常数据
# tuple_security_codes = dbService.get_batch_list_except_security_codes()
tuple_security_codes = [('002390', 'SZ'), ('002539', 'SZ'), ('600268', 'SH'), ('600998', 'SH'), ('600354', 'SH')]
if tuple_security_codes is not None and len(tuple_security_codes) > 0:
    batch_name = 'batch-' + str(batch_num)
    log_name = log_name.format([ma, batch_name])
    print(log_path + log_name)
    logger = Logger(level, log_path, log_name, when, interval, backupCount)
    batchService = StockAverageLineService(DbService(), ma, logger, sleep_seconds, one_time)
    for tuple_security_code in tuple_security_codes:
        security_code = tuple_security_code[0]
        exchange_code = tuple_security_code[1]
        batchService.processing_single_security_code(processing_log_list, security_code, exchange_code, None)


