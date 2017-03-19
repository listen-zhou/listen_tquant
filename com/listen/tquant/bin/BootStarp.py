# coding: utf-8
import threading

import datetime

from com.listen.tquant.service.stock.StockAverageLineService import StockAverageLineService
from com.listen.tquant.service.stock.StockInfoService import StockInfoService
from com.listen.tquant.service.stock.StockDayKlineService import StockDayKlineService
from com.listen.tquant.service.stock.CalendarService import CalendarService


from com.listen.tquant.dbservice.Service import DbService


threads = []

# # 处理股票基本信息线程
dbService1 = DbService()
stockInfoService = StockInfoService(dbService1)
stockInfoThread = threading.Thread(target=stockInfoService.get_stock_info)
stockInfoThread.setName('stockInfoThread')
threads.append(stockInfoThread)

# 处理股票日K数据线程
dbService2 = DbService()
# 全量日K数据处理，False
is_increment = False
stockDayKlineService = StockDayKlineService(dbService2, is_increment)
stockDayKlineThread = threading.Thread(target=stockDayKlineService.processing)
stockDayKlineThread.setName('stockDayKlineThread-is_increment-' + str(is_increment))
threads.append(stockDayKlineThread)

# # 处理证券交易日信息线程
dbService3 = DbService()
calendarService = CalendarService(dbService3)
calendarServiceThread = threading.Thread(target=calendarService.get_calendar_info)
calendarServiceThread.setName('calendarServiceThread')
threads.append(calendarServiceThread)

# 处理股票日均线数据，全部股票
dbService5 = DbService()
ma = 5
stockAverageLineService5 = StockAverageLineService(dbService5, ma)
stockAverageLineServiceThread5 = threading.Thread(target=stockAverageLineService5.processing)
stockAverageLineServiceThread5.setName('stockAverageLineServiceThread5-ma-' + str(ma))
threads.append(stockAverageLineServiceThread5)

# 处理股票日均线数据，单只股票
dbService51 = DbService()
ma = 5
stockAverageLineService51 = StockAverageLineService(dbService51, ma)
stockAverageLineServiceThread51 = threading.Thread(target=stockAverageLineService51.processing_single_security_code,
                                                  args=('000001', 'SZ', 0, datetime.date(1970, 1, 1)))
stockAverageLineServiceThread51.setName('stockAverageLineServiceThread51-ma-' + str(ma))
threads.append(stockAverageLineServiceThread51)


for thread in threads:
    # 设置为守护线程
    thread.setDaemon(False)
    thread.start()


