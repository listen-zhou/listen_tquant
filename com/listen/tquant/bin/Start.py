# coding: utf-8

from com.listen.tquant.log.Logger import Logger

from com.listen.tquant.bin import RunCalendarService
from com.listen.tquant.bin import RunStockAverageLineService
from com.listen.tquant.bin import RunStockDayKlineChangePercentService
from com.listen.tquant.bin import RunStockDayKlineService
from com.listen.tquant.bin import RunStockDeriveKlineService
from com.listen.tquant.bin import RunStockInfoService


logger = Logger()
sleep_seconds = 120
one_time = False

threads = []

# 初始化交易日线程
calendarThreads = RunCalendarService.initThreads(logger, sleep_seconds, True)
if calendarThreads is not None and len(calendarThreads) > 0:
    for thread in calendarThreads:
        threads.append(thread)

# 初始化股票均线处理线程
stockAverageThreads = RunStockAverageLineService.initThreads(logger, sleep_seconds, one_time)
if stockAverageThreads is not None and len(stockAverageThreads) > 0:
    for thread in stockAverageThreads:
        threads.append(thread)


# 初始化股票日K涨跌幅计算线程
stockDayKlineChangePercentThreads = RunStockDayKlineChangePercentService.initThreads(logger, sleep_seconds, one_time)
if stockDayKlineChangePercentThreads is not None and len(stockDayKlineChangePercentThreads) > 0:
    for thread in stockDayKlineChangePercentThreads:
        threads.append(thread)

# 初始化股票日K数据处理线程
stockDayKlineThreads = RunStockDayKlineService.initThreads(logger, sleep_seconds, one_time)
if stockDayKlineThreads is not None and len(stockDayKlineThreads) > 0:
    for thread in stockDayKlineThreads:
        threads.append(thread)

# 初始化股票衍生K线数据线程
stockDeriveKlineThreads = RunStockDeriveKlineService.initThreads(logger, sleep_seconds, one_time)
if stockDeriveKlineThreads is not None and len(stockDeriveKlineThreads) > 0:
    for thread in stockDeriveKlineThreads:
        threads.append(thread)

# 初始化股票基本信息线程
stockInfoThreads = RunStockInfoService.initThreads(logger, sleep_seconds, True)
if stockInfoThreads is not None and len(stockInfoThreads) > 0:
    for thread in stockInfoThreads:
        threads.append(thread)


for thread in threads:
    # 设置为守护线程
    thread.setDaemon(False)
    thread.start()
