# coding: utf-8

from com.listen.tquant.dbservice.Service import DbService
from com.listen.tquant.service.stock.StockDeriveKlineService import StockDeriveKlineService
from com.listen.tquant.log.Logger import Logger
import threading

import logging

log_path = 'd:\\python_log\\derive_kline'
log_name = '\\list_tquant_derive_kline_{0[0]}_{0[1]}.log'
when = 'H'
interval = 1
backupCount = 10
level = logging.INFO
sleep_seconds = 120
one_time = True

batch_num = 0
threads = []
processing_log_list = None

dbService = DbService()

# # 根据股票的日K数据处理生成周（月，季，年）K数据
kline_types = ['week', 'month', 'quarter', 'year']
calendar_max_the_date = dbService.get_calendar_max_the_date()
batch_list = dbService.get_batch_list_security_codes(500)
for kline_type in kline_types:
    batch_name = 'batch-' + str(batch_num)
    log_name = log_name.format([batch_name])
    print(log_path + log_name)
    logger = Logger(level, log_path, log_name, when, interval, backupCount)
    batchService = StockDeriveKlineService(DbService(), kline_type, logger, sleep_seconds, one_time)
    batchServiceThread = threading.Thread(target=batchService.processing_security_codes,
                                          args=(processing_log_list, batch_list[batch_num], batch_name))
    batchServiceThread.setName('StockDeriveKlineService-' + kline_type + '-' + batch_name)
    threads.append(batchServiceThread)

print('batch_list len', len(batch_list), 'batch_num', batch_num)
if len(threads) > 0:
    for thread in threads:
        # 设置为守护线程
        thread.setDaemon(False)
        thread.start()
else:
    print('batch_list len', len(batch_list), 'batch_num', batch_num, 'is to large, exit')