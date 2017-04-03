# coding: utf-8
import threading

import datetime

from com.listen.tquant.service.stock.StockAverageLineService import StockAverageLineService


from com.listen.tquant.dbservice.Service import DbService
from com.listen.tquant.log.Logger import Logger
import logging


threads = []

ma = 3

log_path = 'd:\\python_log\\average'
log_name = '\\list_tquant_average_ma' + str(ma) + '.log'
when = 'H'
interval = 1
backupCount = 10
level = logging.INFO

logger = logger = Logger(level, log_path, log_name, when, interval, backupCount)
sleep_seconds = 120
one_time = True

######################################################################################

# mas = [3, 5, 10, 20, 30, 60, 120, 250]
# mas = [3, 5, 10, 20, 30, 60]
# mas = [5]

stockAverageLineService5 = StockAverageLineService(DbService(), ma, logger, sleep_seconds, one_time)
stockAverageLineServiceThread5 = threading.Thread(target=stockAverageLineService5.loop)
stockAverageLineServiceThread5.setName('stockAverageLineServiceThread-ma-' + str(ma))
threads.append(stockAverageLineServiceThread5)

for thread in threads:
    # 设置为守护线程
    thread.setDaemon(False)
    thread.start()

