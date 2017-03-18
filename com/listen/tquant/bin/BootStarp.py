# coding: utf-8
import threading

from com.listen.tquant.service.stock.StockInfoService import StockInfoService
from com.listen.tquant.service.stock.StockDayKlineService import StockDayKlineService
from com.listen.tquant.service.stock.CalendarService import CalendarService
from com.listen.tquant.service.stock.StockDayKlineIncrementService import StockDayKlineIncrementService

from apscheduler.schedulers.blocking import BlockingScheduler
from com.listen.tquant.dbservice.Service import DbService


threads = []

# dbService1 = DbService()
# stockInfoService = StockInfoService(dbService1)
# stockInfoThread = threading.Thread(target=stockInfoService.get_stock_info)
# stockInfoThread.setName('stockInfoThread')
# threads.append(stockInfoThread)
#
# dbService2 = DbService()
# stockDayKlineService = StockDayKlineService(dbService2)
# stockDayKlineThread = threading.Thread(target=stockDayKlineService.get_all_stock_day_kline)
# stockDayKlineThread.setName('stockDayKlineThread')
# threads.append(stockDayKlineThread)
#
# dbService3 = DbService()
# calendarService = CalendarService(dbService3)
# calendarServiceThread = threading.Thread(target=calendarService.get_calendar_info)
# calendarServiceThread.setName('calendarServiceThread')
# threads.append(calendarServiceThread)

dbService4 = DbService()
stockDayKlineIncrementService = StockDayKlineIncrementService(dbService4)
stockDayKlineIncrementThread = threading.Thread(target=stockDayKlineIncrementService.get_all_stock_day_kline)
stockDayKlineIncrementThread.setName('stockDayKlineIncrementThread')
threads.append(stockDayKlineIncrementThread)



for thread in threads:
    # 设置为守护线程
    thread.setDaemon(False)
    thread.start()


