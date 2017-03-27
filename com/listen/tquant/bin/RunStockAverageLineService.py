# coding: utf-8
import threading

import datetime

from com.listen.tquant.service.stock.StockAverageLineService import StockAverageLineService
from com.listen.tquant.service.stock.StockDayKlineChangePercentService import StockDayKlineChangePercentService
from com.listen.tquant.service.stock.StockInfoService import StockInfoService
from com.listen.tquant.service.stock.StockDayKlineService import StockDayKlineService
from com.listen.tquant.service.stock.CalendarService import CalendarService


from com.listen.tquant.dbservice.Service import DbService
from com.listen.tquant.service.stock.StockDeriveKlineService import StockDeriveKlineService
from com.listen.tquant.log.Logger import Logger
threads = []

logger = Logger()
sleep_seconds = 120
one_time = False

# mas = [5]
# dbServiceMa = DbService()
# stockAverageLineServiceMa = StockAverageLineService(dbServiceMa, mas[0], logger, sleep_seconds, one_time)
# calendar_max_the_date = stockAverageLineServiceMa.get_calendar_max_the_date()
# tuple_security_codes = stockAverageLineServiceMa.dbService.query(stockAverageLineServiceMa.query_stock_sql)
# for ma in mas:
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
#             stockAverageLineServiceMaBatch = StockAverageLineService(DbService(), ma, logger, sleep_seconds, one_time)
#             batch_name = 'batch-' + str(i + 1)
#             stockAverageLineServiceMaBatchThread = threading.Thread(
#                 target=stockAverageLineServiceMaBatch.processing_security_codes,
#                 args=(temp_tuple, calendar_max_the_date, batch_name)
#             )
#             stockAverageLineServiceMaBatchThread.setName('stockAverageLineServiceMaBatchThread-' + batch_name)
#             threads.append(stockAverageLineServiceMaBatchThread)
#             i += 1
######################################################################################
mas = [5, 10, 20, 30, 60, 120, 250]
for ma in mas:
    dbService5 = DbService()
    stockAverageLineService5 = StockAverageLineService(dbService5, ma, logger, sleep_seconds, one_time)
    stockAverageLineServiceThread5 = threading.Thread(target=stockAverageLineService5.loop)
    stockAverageLineServiceThread5.setName('stockAverageLineServiceThread-ma-' + str(ma))
    threads.append(stockAverageLineServiceThread5)

print('threads size:', len(threads))
for thread in threads:
    # 设置为守护线程
    thread.setDaemon(False)
    thread.start()


