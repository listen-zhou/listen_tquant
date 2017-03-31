# coding: utf-8

from com.listen.tquant.dbservice.Service import DbService
from com.listen.tquant.service.stock.StockDeriveKlineService import StockDeriveKlineService
from com.listen.tquant.log.Logger import Logger
import threading

import logging

log_path = 'd:\\python_log\\derive_kline'
log_name = '\\list_tquant_derive_kline.log'
when = 'H'
interval = 1
backupCount = 10
level = logging.INFO

logger = Logger(level, log_path, log_name, when, interval, backupCount)
sleep_seconds = 120
one_time = False

threads = []

# # 根据股票的日K数据处理生成周（月，季，年）K数据
kline_types = ['week', 'month', 'quarter', 'year']
# kline_types = ['week']
for kline_type in kline_types:
    dbServiceweek = DbService()
    stockDeriveKlineServiceweek = StockDeriveKlineService(dbServiceweek, kline_type, logger, sleep_seconds, one_time)
    stockDeriveKlineServiceThreadweek = threading.Thread(target=stockDeriveKlineServiceweek.loop)
    stockDeriveKlineServiceThreadweek.setName('stockDeriveKlineServiceThread-' + kline_type)
    threads.append(stockDeriveKlineServiceThreadweek)
########################################

# 根据股票的日K数据处理生成周（月，季，年）K数据
# kline_types = ['week']
# dbServiceKline = DbService()
# stockDeriveKlineServiceKline = StockDeriveKlineService(dbServiceKline, kline_types[0], logger, sleep_seconds, one_time)
# tuple_security_codes = stockDeriveKlineServiceKline.dbService.query(stockDeriveKlineServiceKline.query_stock_sql)
#
# for kline_type in kline_types:
#     if tuple_security_codes != None and len(tuple_security_codes) > 0:
#         size = len(tuple_security_codes)
#         # 分组的批量大小
#         batch = 1000
#         # 分组后余数
#         remainder = size % batch
#         if remainder > 0:
#             remainder = 1
#         # 分组数，取整数，即批量的倍数
#         multiple = size // batch
#         total = remainder + multiple
#         print('size:', size, 'batch:', batch, 'remainder:', remainder, 'multiple:', multiple, 'total:', total)
#         i = 0
#         while i < total:
#             # 如果是最后一组，则取全量
#             if i == total - 1:
#                 temp_tuple = tuple_security_codes[i * batch:size]
#             else:
#                 temp_tuple = tuple_security_codes[i * batch:(i + 1) * batch]
#             stockDeriveKlineServiceKlineBatch = StockDeriveKlineService(DbService(), kline_type, logger, sleep_seconds, one_time)
#             batch_name = 'batch-' + str(i + 1)
#             stockDeriveKlineServiceKlineBatchThread = threading.Thread(
#                 target=stockDeriveKlineServiceKlineBatch.processing_security_codes,
#                 args=(temp_tuple, batch_name)
#             )
#             stockDeriveKlineServiceKlineBatchThread.setName('stockDeriveKlineServiceKlineBatchThread-' + batch_name)
#             threads.append(stockDeriveKlineServiceKlineBatchThread)
#             i += 1

for thread in threads:
    # 设置为守护线程
    thread.setDaemon(False)
    thread.start()