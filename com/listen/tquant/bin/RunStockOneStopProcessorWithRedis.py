# coding: utf-8

import multiprocessing

from com.listen.tquant.dbservice.Service import DbService
from com.listen.tquant.redis.RedisService import RedisService
from com.listen.tquant.service.stock.StockOneStopProcessorWithRedis import StockOneStopProcessor

def long_time_task_wrapper(cls_instance):
    print('long_time_task_wrapper init ...')
    cls_instance.processing_single_security_code()

def long_time_task_redis_read(cls_instance):
    print('long_time_task_redis_read init ...')
    cls_instance.get_log()

def long_time_task_consume(cls_instance):
    cls_instance.consume()

if __name__ == '__main__':
    cpu_count = multiprocessing.cpu_count()
    manager = multiprocessing.Manager()
    pool = manager.Pool()
    pool = multiprocessing.Pool(processes=cpu_count)

    dbService = DbService()
    securty_codes = dbService.get_worth_buying_stock()
    redisService = RedisService()
    mas = [3, 5,10]
    service = StockOneStopProcessor(securty_codes[0], mas)
    pool.apply_async(long_time_task_consume, args=(service, ))
    print('cpu_count', cpu_count, 'securty_codes len', len(securty_codes))
    securty_codes = securty_codes[0:10]
    if securty_codes is not None:
        securty_codes = [securty_codes[i][0] for i in range(len(securty_codes))]
        print('securty_codes after len', len(securty_codes))
        for securty_code in securty_codes:
            service = StockOneStopProcessor(securty_code, mas)
            pool.apply_async(long_time_task_wrapper, args=(service, ))
        pool.close()
        pool.join()
        print('done')