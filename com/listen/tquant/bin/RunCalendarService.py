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
sleep_seconds = 43200
one_time = True


# 处理证券交易日信息线程
dbService3 = DbService()
calendarService = CalendarService(dbService3, logger, sleep_seconds, one_time)
calendarServiceThread = threading.Thread(target=calendarService.loop)
calendarServiceThread.setName('calendarServiceThread')
threads.append(calendarServiceThread)
#####################################################################################


print('threads size:', len(threads))
for thread in threads:
    # 设置为守护线程
    thread.setDaemon(False)
    thread.start()


