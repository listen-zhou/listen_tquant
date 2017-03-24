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

# # 处理股票基本信息线程
# dbService1 = DbService()
# stockInfoService = StockInfoService(dbService1, logger)
# stockInfoThread = threading.Thread(target=stockInfoService.processing)
# stockInfoThread.setName('stockInfoThread')
# threads.append(stockInfoThread)
#
# # 处理证券交易日信息线程
# dbService3 = DbService()
# calendarService = CalendarService(dbService3, logger)
# calendarServiceThread = threading.Thread(target=calendarService.processing)
# calendarServiceThread.setName('calendarServiceThread')
# threads.append(calendarServiceThread)
#
# # 处理股票日K数据线程
# dbService2 = DbService()
# # 全量日K数据处理，False
# stockDayKlineService = StockDayKlineService(dbService2, logger)
# stockDayKlineThread = threading.Thread(target=stockDayKlineService.processing)
# stockDayKlineThread.setName('stockDayKlineThread')
# threads.append(stockDayKlineThread)
# 处理股票日K涨跌幅数据
dbService6 = DbService()
stockDayKlineChangePercentService = StockDayKlineChangePercentService(dbService6, logger)
stockDayKlineChangePercentServiceThread = threading.Thread(target=stockDayKlineChangePercentService.processing)
stockDayKlineChangePercentServiceThread.setName('stockDayKlineChangePercentServiceThread')
threads.append(stockDayKlineChangePercentServiceThread)
#
# # 处理股票日均线数据，全部股票
# dbService5 = DbService()
# ma = 5
# stockAverageLineService5 = StockAverageLineService(dbService5, ma, logger)
# stockAverageLineServiceThread5 = threading.Thread(target=stockAverageLineService5.processing)
# stockAverageLineServiceThread5.setName('stockAverageLineServiceThread-ma-' + str(ma))
# threads.append(stockAverageLineServiceThread5)
#
# dbService10 = DbService()
# ma = 10
# stockAverageLineService10 = StockAverageLineService(dbService10, ma, logger)
# stockAverageLineServiceThread10 = threading.Thread(target=stockAverageLineService10.processing)
# stockAverageLineServiceThread10.setName('stockAverageLineServiceThread-ma-' + str(ma))
# threads.append(stockAverageLineServiceThread10)
#
# dbService20 = DbService()
# ma = 20
# stockAverageLineService20 = StockAverageLineService(dbService20, ma, logger)
# stockAverageLineServiceThread20 = threading.Thread(target=stockAverageLineService20.processing)
# stockAverageLineServiceThread20.setName('stockAverageLineServiceThread-ma-' + str(ma))
# threads.append(stockAverageLineServiceThread20)
#
# dbService30 = DbService()
# ma = 30
# stockAverageLineService30 = StockAverageLineService(dbService30, ma, logger)
# stockAverageLineServiceThread30 = threading.Thread(target=stockAverageLineService30.processing)
# stockAverageLineServiceThread30.setName('stockAverageLineServiceThread-ma-' + str(ma))
# threads.append(stockAverageLineServiceThread30)
#
# dbService60 = DbService()
# ma = 60
# stockAverageLineService60 = StockAverageLineService(dbService60, ma, logger)
# stockAverageLineServiceThread60 = threading.Thread(target=stockAverageLineService60.processing)
# stockAverageLineServiceThread60.setName('stockAverageLineServiceThread-ma-' + str(ma))
# threads.append(stockAverageLineServiceThread60)
#
# dbService120 = DbService()
# ma = 120
# stockAverageLineService120 = StockAverageLineService(dbService120, ma, logger)
# stockAverageLineServiceThread120 = threading.Thread(target=stockAverageLineService120.processing)
# stockAverageLineServiceThread120.setName('stockAverageLineServiceThread-ma-' + str(ma))
# threads.append(stockAverageLineServiceThread120)
#
# dbService250 = DbService()
# ma = 250
# stockAverageLineService250 = StockAverageLineService(dbService250, ma, logger)
# stockAverageLineServiceThread250 = threading.Thread(target=stockAverageLineService250.processing)
# stockAverageLineServiceThread250.setName('stockAverageLineServiceThread-ma-' + str(ma))
# threads.append(stockAverageLineServiceThread250)
#
# # # 根据股票的日K数据处理生成周K数据
# dbServiceweek = DbService()
# kline_type = 'week'
# stockDeriveKlineServiceweek = StockDeriveKlineService(dbServiceweek, kline_type, logger)
# stockDeriveKlineServiceThreadweek = threading.Thread(target=stockDeriveKlineServiceweek.processing)
# stockDeriveKlineServiceThreadweek.setName('stockDeriveKlineServiceThread-' + kline_type)
# threads.append(stockDeriveKlineServiceThreadweek)
#
# dbServicemonth = DbService()
# kline_type = 'month'
# stockDeriveKlineServicemonth = StockDeriveKlineService(dbServicemonth, kline_type, logger)
# stockDeriveKlineServiceThreadmonth = threading.Thread(target=stockDeriveKlineServicemonth.processing)
# stockDeriveKlineServiceThreadmonth.setName('stockDeriveKlineServiceThread-' + kline_type)
# threads.append(stockDeriveKlineServiceThreadmonth)
#
# dbServicequarter = DbService()
# kline_type = 'quarter'
# stockDeriveKlineServicequarter = StockDeriveKlineService(dbServicequarter, kline_type, logger)
# stockDeriveKlineServiceThreadquarter = threading.Thread(target=stockDeriveKlineServicequarter.processing)
# stockDeriveKlineServiceThreadquarter.setName('stockDeriveKlineServiceThread-' + kline_type)
# threads.append(stockDeriveKlineServiceThreadquarter)
#
# dbServiceyear = DbService()
# kline_type = 'year'
# stockDeriveKlineServiceyear = StockDeriveKlineService(dbServiceyear, kline_type, logger)
# stockDeriveKlineServiceThreadyear = threading.Thread(target=stockDeriveKlineServiceyear.processing)
# stockDeriveKlineServiceThreadyear.setName('stockDeriveKlineServiceThread-' + kline_type)
# threads.append(stockDeriveKlineServiceThreadyear)

for thread in threads:
    # 设置为守护线程
    thread.setDaemon(False)
    thread.start()


