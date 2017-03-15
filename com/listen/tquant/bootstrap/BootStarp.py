# coding: utf-8
import threading

from com.listen.tquant.service.StockService import StockInfoService
from com.listen.tquant.service.StockService import StockDayKlineService

from apscheduler.schedulers.blocking import BlockingScheduler
from com.listen.tquant.dbservice.Service import DbService


dbService1 = DbService()
dbService2 = DbService()
stockInfoService = StockInfoService(dbService1)
stockDayKlineService = StockDayKlineService(dbService2)

threads = []
stockInfoThread = threading.Thread(target=stockInfoService.get_stock_info)
stockInfoThread.setName('stockInfoThread')
threads.append(stockInfoThread)

stockDayKlineThread = threading.Thread(target=stockDayKlineService.get_all_stock_day_kline)
stockDayKlineThread.setName('stockDayKlineThread')
threads.append(stockDayKlineThread)

for thread in threads:
    # 设置为守护线程
    thread.setDaemon(False)
    thread.start()
print('Done')

# if __name__ == '__main__':
#     scheduler = BlockingScheduler()
#     dbService = DbService()
#     stockInfoService = StockInfoService(dbService)
#     stockDayKlineService = StockDayKlineService(dbService)
#     # 定时任务，每隔1分钟执行一次StockInfoService的run方法
#     # scheduler.add_job(stockInfoService.get_stock_info, 'interval', minutes=1)
#     # scheduler.add_job(stockInfoService.get_all_stock_day_kline(), 'interval', minutes=1)
#     stockInfoService.get_stock_info()
#     stockDayKlineService.get_all_stock_day_kline()
#     try:
#         scheduler.start()
#     except (KeyboardInterrupt, SystemExit):
#         scheduler.shutdown()
#         print('exit')


