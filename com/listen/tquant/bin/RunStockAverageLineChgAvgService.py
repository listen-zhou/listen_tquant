# coding: utf-8
import threading

import datetime

from com.listen.tquant.service.stock.StockAverageLineChgAvgService import StockAverageLineChgAvgService


from com.listen.tquant.dbservice.Service import DbService
from com.listen.tquant.log.Logger import Logger
import logging


threads = []


log_path = 'd:\\python_log\\average_chg_avg'
log_name = '\\list_tquant_average_chg_avg.log'
when = 'H'
interval = 1
backupCount = 10
level = logging.INFO

logger = logger = Logger(level, log_path, log_name, when, interval, backupCount)
sleep_seconds = 120
one_time = False

# mas = [3, 5, 10, 20, 30, 60, 120, 250]
mas = [3, 5, 10]
# mas = [5]
security_codes_log_list = None
security_code = '002460'
# security_code = '002466'
exchange_code = 'SZ'

security_code = '600519'
exchange_code = 'SH'

decline_ma_the_date = None
for ma in mas:
    dbService5 = DbService()
    stockAverageLineService5 = StockAverageLineChgAvgService(dbService5, ma, logger, sleep_seconds, one_time)
    stockAverageLineServiceThread5 = threading.Thread(target=stockAverageLineService5.processing_single_security_code,
                                                      args=(security_codes_log_list, security_code, exchange_code, decline_ma_the_date))
    stockAverageLineServiceThread5.setName('stockAverageLineServiceThread-ma-' + str(ma))
    threads.append(stockAverageLineServiceThread5)

for thread in threads:
    # 设置为守护线程
    thread.setDaemon(False)
    thread.start()

