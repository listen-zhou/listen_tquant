# coding: utf-8

from com.listen.tquant.service.Service import StockInfoService

from apscheduler.schedulers.blocking import BlockingScheduler
from com.listen.tquant.dbservice.Service import DbService


if __name__ == '__main__':
    scheduler = BlockingScheduler()
    dbService = DbService()
    stockInfoService = StockInfoService(dbService)
    # 定时任务，每隔1分钟执行一次StockInfoService的run方法
    # scheduler.add_job(stockInfoService.get_stock_info, 'interval', minutes=1)
    scheduler.add_job(stockInfoService.get_all_stock_day_kline(), 'interval', minutes=1)
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        print('exit')


