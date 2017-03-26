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

# # # 处理股票基本信息线程
# dbService1 = DbService()
# stockInfoService = StockInfoService(dbService1, logger, sleep_seconds)
# stockInfoThread = threading.Thread(target=stockInfoService.loop)
# stockInfoThread.setName('stockInfoThread')
# threads.append(stockInfoThread)
# #
# # # 处理证券交易日信息线程
# dbService3 = DbService()
# calendarService = CalendarService(dbService3, logger, sleep_seconds)
# calendarServiceThread = threading.Thread(target=calendarService.loop)
# calendarServiceThread.setName('calendarServiceThread')
# threads.append(calendarServiceThread)
#
# # 处理股票日K数据线程
# dbService2 = DbService()
# # 全量日K数据处理，False
# stockDayKlineService = StockDayKlineService(dbService2, logger, sleep_seconds)
# stockDayKlineThread = threading.Thread(target=stockDayKlineService.loop)
# stockDayKlineThread.setName('stockDayKlineThread')
# threads.append(stockDayKlineThread)

# # 多线程处理全部日K数据，分组处理
# dbServiceBatch = DbService()
# stockDayKlineService = StockDayKlineService(dbServiceBatch, logger, sleep_seconds)
# calendar_max_the_date = stockDayKlineService.get_calendar_max_the_date()
# tuple_security_codes = stockDayKlineService.dbService.query(stockDayKlineService.query_stock_sql)
# if tuple_security_codes != None and len(tuple_security_codes) > 0:
#     size = len(tuple_security_codes)
#     # 分组的批量大小
#     batch = 30
#     # 分组后余数
#     remainder = size % batch
#     if remainder > 0:
#         remainder = 1
#     # 分组数，取整数，即批量的倍数
#     multiple = size // batch
#     total = remainder + multiple
#     print('size:', size, 'batch:', batch, 'remainder:', remainder, 'multiple:', multiple, 'total:', total)
#     i = 0
#     while i < total:
#         # 如果是最后一组，则取全量
#         if i == total - 1:
#             temp_tuple = tuple_security_codes[i*batch:size]
#         else:
#             temp_tuple = tuple_security_codes[i*batch:(i+1)*batch]
#         stockDayKlineThread = threading.Thread(target=stockDayKlineService.processing_security_codes(temp_tuple, calendar_max_the_date, 'batch-' + str(i+1)))
#         stockDayKlineThread.setName('stockDayKlineThread-batch' + str(i+1))
#         threads.append(stockDayKlineThread)
#         i += 1


# # 处理股票日K涨跌幅数据
dbService6 = DbService()
stockDayKlineChangePercentService = StockDayKlineChangePercentService(dbService6, logger, sleep_seconds)
stockDayKlineChangePercentServiceThread = threading.Thread(target=stockDayKlineChangePercentService.loop)
stockDayKlineChangePercentServiceThread.setName('stockDayKlineChangePercentServiceThread')
threads.append(stockDayKlineChangePercentServiceThread)
#
# # 处理股票日均线数据，全部股票
dbService5 = DbService()
ma = 5
stockAverageLineService5 = StockAverageLineService(dbService5, ma, logger, sleep_seconds)
stockAverageLineServiceThread5 = threading.Thread(target=stockAverageLineService5.loop)
stockAverageLineServiceThread5.setName('stockAverageLineServiceThread-ma-' + str(ma))
threads.append(stockAverageLineServiceThread5)
#
dbService10 = DbService()
ma = 10
stockAverageLineService10 = StockAverageLineService(dbService10, ma, logger, sleep_seconds)
stockAverageLineServiceThread10 = threading.Thread(target=stockAverageLineService10.loop)
stockAverageLineServiceThread10.setName('stockAverageLineServiceThread-ma-' + str(ma))
threads.append(stockAverageLineServiceThread10)
#
dbService20 = DbService()
ma = 20
stockAverageLineService20 = StockAverageLineService(dbService20, ma, logger, sleep_seconds)
stockAverageLineServiceThread20 = threading.Thread(target=stockAverageLineService20.loop)
stockAverageLineServiceThread20.setName('stockAverageLineServiceThread-ma-' + str(ma))
threads.append(stockAverageLineServiceThread20)
#
dbService30 = DbService()
ma = 30
stockAverageLineService30 = StockAverageLineService(dbService30, ma, logger, sleep_seconds)
stockAverageLineServiceThread30 = threading.Thread(target=stockAverageLineService30.loop)
stockAverageLineServiceThread30.setName('stockAverageLineServiceThread-ma-' + str(ma))
threads.append(stockAverageLineServiceThread30)
#
dbService60 = DbService()
ma = 60
stockAverageLineService60 = StockAverageLineService(dbService60, ma, logger, sleep_seconds)
stockAverageLineServiceThread60 = threading.Thread(target=stockAverageLineService60.loop)
stockAverageLineServiceThread60.setName('stockAverageLineServiceThread-ma-' + str(ma))
threads.append(stockAverageLineServiceThread60)
#
dbService120 = DbService()
ma = 120
stockAverageLineService120 = StockAverageLineService(dbService120, ma, logger, sleep_seconds)
stockAverageLineServiceThread120 = threading.Thread(target=stockAverageLineService120.loop)
stockAverageLineServiceThread120.setName('stockAverageLineServiceThread-ma-' + str(ma))
threads.append(stockAverageLineServiceThread120)
#
dbService250 = DbService()
ma = 250
stockAverageLineService250 = StockAverageLineService(dbService250, ma, logger, sleep_seconds)
stockAverageLineServiceThread250 = threading.Thread(target=stockAverageLineService250.loop)
stockAverageLineServiceThread250.setName('stockAverageLineServiceThread-ma-' + str(ma))
threads.append(stockAverageLineServiceThread250)
#
# # # 根据股票的日K数据处理生成周K数据
dbServiceweek = DbService()
kline_type = 'week'
stockDeriveKlineServiceweek = StockDeriveKlineService(dbServiceweek, kline_type, logger, sleep_seconds)
stockDeriveKlineServiceThreadweek = threading.Thread(target=stockDeriveKlineServiceweek.loop)
stockDeriveKlineServiceThreadweek.setName('stockDeriveKlineServiceThread-' + kline_type)
threads.append(stockDeriveKlineServiceThreadweek)
#
dbServicemonth = DbService()
kline_type = 'month'
stockDeriveKlineServicemonth = StockDeriveKlineService(dbServicemonth, kline_type, logger, sleep_seconds)
stockDeriveKlineServiceThreadmonth = threading.Thread(target=stockDeriveKlineServicemonth.loop)
stockDeriveKlineServiceThreadmonth.setName('stockDeriveKlineServiceThread-' + kline_type)
threads.append(stockDeriveKlineServiceThreadmonth)
#
dbServicequarter = DbService()
kline_type = 'quarter'
stockDeriveKlineServicequarter = StockDeriveKlineService(dbServicequarter, kline_type, logger, sleep_seconds)
stockDeriveKlineServiceThreadquarter = threading.Thread(target=stockDeriveKlineServicequarter.loop)
stockDeriveKlineServiceThreadquarter.setName('stockDeriveKlineServiceThread-' + kline_type)
threads.append(stockDeriveKlineServiceThreadquarter)
#
dbServiceyear = DbService()
kline_type = 'year'
stockDeriveKlineServiceyear = StockDeriveKlineService(dbServiceyear, kline_type, logger, sleep_seconds)
stockDeriveKlineServiceThreadyear = threading.Thread(target=stockDeriveKlineServiceyear.loop)
stockDeriveKlineServiceThreadyear.setName('stockDeriveKlineServiceThread-' + kline_type)
threads.append(stockDeriveKlineServiceThreadyear)

print('threads size:', len(threads))
for thread in threads:
    # 设置为守护线程
    thread.setDaemon(False)
    thread.start()


