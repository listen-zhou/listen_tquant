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


def initThreads(logger, sleep_seconds, one_time):

    # 处理股票基本信息线程
    dbService1 = DbService()
    stockInfoService = StockInfoService(dbService1, logger, sleep_seconds, one_time)
    stockInfoThread = threading.Thread(target=stockInfoService.loop)
    stockInfoThread.setName('stockInfoThread')
    threads.append(stockInfoThread)
    return threads
####################################################################################


