# coding: utf-8
import threading

import datetime

from com.listen.tquant.service.stock.StockDayKlineService import StockDayKlineService


from com.listen.tquant.dbservice.Service import DbService
from com.listen.tquant.log.Logger import Logger
threads = []


# # 处理股票日K数据线程
# dbService2 = DbService()
# # 全量日K数据处理，False
# stockDayKlineService = StockDayKlineService(dbService2, logger, sleep_seconds, one_time)
# stockDayKlineThread = threading.Thread(target=stockDayKlineService.loop)
# stockDayKlineThread.setName('stockDayKlineThread')
# threads.append(stockDayKlineThread)
###########################################################################################

def initThreads(logger, sleep_seconds, one_time):
    # 多线程处理全部日K数据，分组处理
    dbServiceBatch = DbService()
    stockDayKlineService = StockDayKlineService(dbServiceBatch, logger, sleep_seconds, one_time)
    calendar_max_the_date = stockDayKlineService.get_calendar_max_the_date()
    tuple_security_codes = stockDayKlineService.dbService.query(stockDayKlineService.query_stock_sql)
    if tuple_security_codes is not None and len(tuple_security_codes) > 0:
        size = len(tuple_security_codes)
        # 分组的批量大小
        batch = 500
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
                temp_tuple = tuple_security_codes[i*batch:size]
            else:
                temp_tuple = tuple_security_codes[i*batch:(i+1)*batch]
            stockDayKlineServiceBatch = StockDayKlineService(DbService(), logger, sleep_seconds, one_time)
            stockDayKlineThread = threading.Thread(target=stockDayKlineServiceBatch.processing_security_codes, args=(None, temp_tuple, calendar_max_the_date, 'batch-' + str(i+1)))
            stockDayKlineThread.setName('stockDayKlineThread-batch' + str(i+1))
            threads.append(stockDayKlineThread)
            i += 1
    return threads
#############################################################################################


