# coding: utf-8
import threading

from com.listen.tquant.service.stock.CalendarService import CalendarService


from com.listen.tquant.dbservice.Service import DbService
from com.listen.tquant.log.Logger import Logger
threads = []


def initThreads(logger, sleep_seconds, one_time):
    # 处理证券交易日信息线程
    dbService3 = DbService()
    calendarService = CalendarService(dbService3, logger, sleep_seconds, one_time)
    calendarServiceThread = threading.Thread(target=calendarService.loop)
    calendarServiceThread.setName('calendarServiceThread')
    threads.append(calendarServiceThread)

    return threads