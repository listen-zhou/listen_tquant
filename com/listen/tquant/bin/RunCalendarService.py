# coding: utf-8
import threading

import logging

from com.listen.tquant.service.stock.CalendarService import CalendarService


from com.listen.tquant.dbservice.Service import DbService
from com.listen.tquant.log.Logger import Logger
threads = []

log_path = 'd:\\python_log\\calendar'
log_name = '\\list_tquant_calendar.log'
when = 'H'
interval = 1
backupCount = 10
level = logging.INFO

logger = Logger(level, log_path, log_name, when, interval, backupCount)
sleep_seconds = 120
one_time = True

# 处理证券交易日信息线程
dbService3 = DbService()
calendarService = CalendarService(dbService3, logger, sleep_seconds, one_time)
calendarServiceThread = threading.Thread(target=calendarService.loop)
calendarServiceThread.setName('calendarServiceThread')
threads.append(calendarServiceThread)

for thread in threads:
    # 设置为守护线程
    thread.setDaemon(False)
    thread.start()


