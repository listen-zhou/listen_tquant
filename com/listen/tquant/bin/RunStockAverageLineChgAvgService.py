# coding: utf-8
import threading

import datetime

from com.listen.tquant.service.stock.StockAverageLineChgAvgService import StockAverageLineChgAvgService


from com.listen.tquant.dbservice.Service import DbService
from com.listen.tquant.log.Logger import Logger
import logging


log_path = 'd:\\python_log\\average_chg_avg'
log_name = '\\list_tquant_average_chg_avg_ma{0[0]}_{0[1]}.log'
when = 'H'
interval = 1
backupCount = 10
level = logging.INFO
sleep_seconds = 120
one_time = True
threads = []
processing_log_list = None

dbService = DbService()

# 获取股票代码按batch_size分批后的list
batch_list = dbService.get_batch_list_security_codes(500)
calendar_max_the_date = dbService.get_calendar_max_the_date()


batch_num = 6
ma = 10

if batch_num < len(batch_list):
    batch_name = 'batch-' + str(batch_num)
    log_name = log_name.format([ma, batch_name])
    print(log_path + log_name)
    logger = Logger(level, log_path, log_name, when, interval, backupCount)
    batchService = StockAverageLineChgAvgService(DbService(), ma, logger, sleep_seconds, one_time)
    batchServiceThread = threading.Thread(target=batchService.processing_security_codes,
                                          args=(processing_log_list, batch_list[batch_num],
                                                calendar_max_the_date, batch_name
                                                )
                                          )
    batchServiceThread.setName('StockAverageLineChgAvgService-ma' + str(ma) + '_' + batch_name)
    threads.append(batchServiceThread)

print('batch_list len', len(batch_list), 'batch_num', batch_num)
if len(threads) > 0:
    for thread in threads:
        # 设置为守护线程
        thread.setDaemon(False)
        thread.start()
else:
    print('batch_list len', len(batch_list), 'batch_num', batch_num, 'is to large, exit')
############################################################









# tuple_security_codes = dbService.query_all_security_codes()
# # mas = [3, 5, 10, 20, 30, 60, 120, 250]
# mas = [3, 5, 10]
# for ma in mas:
#     batch_name = 'batch-' + str(batch_num)
#     log_name = log_name.format([ma, batch_name])
#     print(log_path + log_name)
#     logger = Logger(level, log_path, log_name, when, interval, backupCount)
#     singleService = StockAverageLineChgAvgService(DbService(), ma, logger, sleep_seconds, one_time)
#     for item in tuple_security_codes:
#         security_code = item[0]
#         exchange_code = item[1]
#         decline_ma_the_date = None
#         singleService.processing_single_security_code(processing_log_list, security_code, exchange_code, decline_ma_the_date)


