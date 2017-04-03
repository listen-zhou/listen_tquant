# coding: utf-8
import threading

import datetime

from com.listen.tquant.service.stock.StockDayKlineChangePercentService import StockDayKlineChangePercentService


from com.listen.tquant.dbservice.Service import DbService
from com.listen.tquant.log.Logger import Logger
import logging

threads = []

batch_num = 6

log_path = 'd:\\python_log\\change_percent'
log_name = '\\list_tquant_change_percent_' + str(batch_num + 1) + '.log'
when = 'H'
interval = 1
backupCount = 10
level = logging.INFO

logger = Logger(level, log_path, log_name, when, interval, backupCount)
sleep_seconds = 120
one_time = False

# # 处理股票日K涨跌幅数据
# dbService6 = DbService()
# stockDayKlineChangePercentService6 = StockDayKlineChangePercentService(dbService6, logger, sleep_seconds, one_time)
# stockDayKlineChangePercentService6Thread = threading.Thread(target=stockDayKlineChangePercentService6.loop)
# stockDayKlineChangePercentService6Thread.setName('stockDayKlineChangePercentService6Thread')
# threads.append(stockDayKlineChangePercentService6Thread)
#############################################################################

# # 多线程处理全部日K涨跌幅数据，分组处理
processing_log_list = None
dbServiceChange = DbService()
stockDayKlineChangePercentService = StockDayKlineChangePercentService(dbServiceChange, logger, sleep_seconds, one_time)
tuple_security_codes = stockDayKlineChangePercentService.dbService.query(stockDayKlineChangePercentService.query_stock_sql)
batch_list = []

if tuple_security_codes != None and len(tuple_security_codes) > 0:
    size = len(tuple_security_codes)
    # 分组的批量大小
    batch = 500
    # 分组后余数
    remainder = size % batch
    if remainder > 0:
        remainder = 1
    # 分组数，取整数，即批量的倍数
    multiple = size // batch
    total = remainder + multiple
    print('size:', size, 'batch:', batch, 'remainder:', remainder, 'multiple:', multiple, 'total:', total)
    i = 0
    while i < total:
        # 如果是最后一组，则取全量
        if i == total - 1:
            temp_tuple = tuple_security_codes[i * batch:size]
        else:
            temp_tuple = tuple_security_codes[i * batch:(i + 1) * batch]
        batch_list.append(temp_tuple)
        i += 1
# #######################################################################################################
print('batch_list len', len(batch_list), 'batch_num', batch_num)
if batch_num < len(batch_list):
    batch_name = 'batch-' + str(batch_num + 1)
    stockDayKlineChangePercentServiceBatch = StockDayKlineChangePercentService(DbService(), logger, sleep_seconds, one_time)
    stockDayKlineChangePercentServiceBatchThread = threading.Thread(
        target=stockDayKlineChangePercentServiceBatch.processing_security_codes,
        args=(processing_log_list, batch_list[batch_num], batch_name)
    )
    stockDayKlineChangePercentServiceBatchThread.setName('stockDayKlineChangePercentServiceBatchThread-' + batch_name)
    threads.append(stockDayKlineChangePercentServiceBatchThread)

if len(threads) > 0:
    for thread in threads:
        # 设置为守护线程
        thread.setDaemon(False)
        thread.start()
else:
    print('batch_list len', len(batch_list), 'batch_num', batch_num, 'is to large, exit')
