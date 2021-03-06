# coding: utf-8

import multiprocessing
import time
from com.listen.tquant.dbservice.Service import DbService
from com.listen.tquant.service.stock.StockOneTable import StockOneTable
from com.listen.tquant.utils.Utils import Utils


def long_time_task_wrapper(cls_instance):
    cls_instance.processing_single_security_code()

if __name__ == '__main__':
    flag = False
    while True:
        cpu_count = multiprocessing.cpu_count()
        manager = multiprocessing.Manager()
        pool = manager.Pool()
        pool = multiprocessing.Pool(processes=4)

        dbService = DbService()
        securty_codes = dbService.get_worth_buying_stock()
        # securty_codes = (('002415', ), )
        print('cpu_count', cpu_count, 'security_codes len', len(securty_codes))
        if securty_codes is not None:
            securty_codes = [securty_codes[i][0] for i in range(len(securty_codes))]
            print('security_codes after len', len(securty_codes))
            for securty_code in securty_codes:
                service = StockOneTable(securty_code, False, flag)
                pool.apply_async(long_time_task_wrapper, args=(service, ))
                # break
            pool.close()
            pool.join()
            print('done')
            flag = False
        time.sleep(240)