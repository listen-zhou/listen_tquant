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
one_time = True

mas = [5]
dbServiceMa = DbService()
stockAverageLineServiceMa = StockAverageLineService(dbServiceMa, mas[0], logger, sleep_seconds, one_time)
calendar_max_the_date = stockAverageLineServiceMa.get_calendar_max_the_date()
tuple_security_codes = stockAverageLineServiceMa.dbService.query(stockAverageLineServiceMa.query_stock_sql)
for ma in mas:
    if tuple_security_codes != None and len(tuple_security_codes) > 0:
        size = len(tuple_security_codes)
        # 分组的批量大小
        batch = 1000
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
            stockAverageLineServiceMaBatch = StockAverageLineService(DbService(), ma, logger, sleep_seconds, one_time)
            batch_name = 'batch-' + str(i + 1)
            stockAverageLineServiceMaBatchThread = threading.Thread(
                target=stockAverageLineServiceMaBatch.processing_security_codes,
                args=(temp_tuple, calendar_max_the_date, batch_name)
            )
            stockAverageLineServiceMaBatchThread.setName('stockAverageLineServiceMaBatchThread-' + batch_name)
            threads.append(stockAverageLineServiceMaBatchThread)
            i += 1
######################################################################################

# # 处理股票日均线数据，全部股票
# dbService5 = DbService()
# ma = 5
# stockAverageLineService5 = StockAverageLineService(dbService5, ma, logger, sleep_seconds, one_time)
# stockAverageLineServiceThread5 = threading.Thread(target=stockAverageLineService5.loop)
# stockAverageLineServiceThread5.setName('stockAverageLineServiceThread-ma-' + str(ma))
# threads.append(stockAverageLineServiceThread5)
# #
# dbService10 = DbService()
# ma = 10
# stockAverageLineService10 = StockAverageLineService(dbService10, ma, logger, sleep_seconds, one_time)
# stockAverageLineServiceThread10 = threading.Thread(target=stockAverageLineService10.loop)
# stockAverageLineServiceThread10.setName('stockAverageLineServiceThread-ma-' + str(ma))
# threads.append(stockAverageLineServiceThread10)
# #
# dbService20 = DbService()
# ma = 20
# stockAverageLineService20 = StockAverageLineService(dbService20, ma, logger, sleep_seconds, one_time)
# stockAverageLineServiceThread20 = threading.Thread(target=stockAverageLineService20.loop)
# stockAverageLineServiceThread20.setName('stockAverageLineServiceThread-ma-' + str(ma))
# threads.append(stockAverageLineServiceThread20)
# #
# dbService30 = DbService()
# ma = 30
# stockAverageLineService30 = StockAverageLineService(dbService30, ma, logger, sleep_seconds, one_time)
# stockAverageLineServiceThread30 = threading.Thread(target=stockAverageLineService30.loop)
# stockAverageLineServiceThread30.setName('stockAverageLineServiceThread-ma-' + str(ma))
# threads.append(stockAverageLineServiceThread30)
# #
# dbService60 = DbService()
# ma = 60
# stockAverageLineService60 = StockAverageLineService(dbService60, ma, logger, sleep_seconds, one_time)
# stockAverageLineServiceThread60 = threading.Thread(target=stockAverageLineService60.loop)
# stockAverageLineServiceThread60.setName('stockAverageLineServiceThread-ma-' + str(ma))
# threads.append(stockAverageLineServiceThread60)
# #
# dbService120 = DbService()
# ma = 120
# stockAverageLineService120 = StockAverageLineService(dbService120, ma, logger, sleep_seconds, one_time)
# stockAverageLineServiceThread120 = threading.Thread(target=stockAverageLineService120.loop)
# stockAverageLineServiceThread120.setName('stockAverageLineServiceThread-ma-' + str(ma))
# threads.append(stockAverageLineServiceThread120)
# #
# dbService250 = DbService()
# ma = 250
# stockAverageLineService250 = StockAverageLineService(dbService250, ma, logger, sleep_seconds, one_time)
# stockAverageLineServiceThread250 = threading.Thread(target=stockAverageLineService250.loop)
# stockAverageLineServiceThread250.setName('stockAverageLineServiceThread-ma-' + str(ma))
# threads.append(stockAverageLineServiceThread250)

print('threads size:', len(threads))
for thread in threads:
    # 设置为守护线程
    thread.setDaemon(False)
    thread.start()


