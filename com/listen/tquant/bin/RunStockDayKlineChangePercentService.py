# coding: utf-8
import threading

import datetime

from com.listen.tquant.service.stock.StockDayKlineChangePercentService import StockDayKlineChangePercentService


from com.listen.tquant.dbservice.Service import DbService
from com.listen.tquant.log.Logger import Logger
import logging


log_path = 'd:\\python_log\\change_percent'
log_name = '\\list_tquant_change_percent_{0[0]}.log'
when = 'H'
interval = 1
backupCount = 10
level = logging.INFO
sleep_seconds = 120
one_time = False

batch_num = 6
threads = []
processing_log_list = None

dbService = DbService()

# # 多线程处理全部日K涨跌幅数据，分组处理
calendar_max_the_date = dbService.get_calendar_max_the_date()
batch_list = dbService.get_batch_list_security_codes(500)
if batch_num < len(batch_list):
    batch_name = 'batch-' + str(batch_num)
    log_name = log_name.format([batch_name])
    print(log_path + log_name)
    logger = Logger(level, log_path, log_name, when, interval, backupCount)
    batchService = StockDayKlineChangePercentService(DbService(), logger, sleep_seconds, one_time)
    batchServiceThread = threading.Thread(
        target=batchService.processing_security_codes,
        args=(processing_log_list, batch_list[batch_num], batch_name)
    )
    batchServiceThread.setName('StockDayKlineChangePercentService-' + batch_name)
    threads.append(batchServiceThread)

print('batch_list len', len(batch_list), 'batch_num', batch_num)
if len(threads) > 0:
    for thread in threads:
        # 设置为守护线程
        thread.setDaemon(False)
        thread.start()
else:
    print('batch_list len', len(batch_list), 'batch_num', batch_num, 'is to large, exit')
