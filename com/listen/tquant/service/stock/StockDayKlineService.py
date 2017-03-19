# coding: utf-8
import traceback
from decimal import *
import types

import numpy
import tquant as tt
import datetime
import time


class StockDayKlineService():
    """
    股票日K数据处理服务
    """
    def __init__(self, dbService, is_increment):
        self.serviceName = 'StockDayKlineService'
        self.business_type = 'STOCK_DAY_KLINE'
        self.is_increment = is_increment
        self.dbService = dbService
        print(datetime.datetime.now(), self.serviceName, 'is_increment', self.is_increment, 'init ...', datetime.datetime.now())
        if is_increment:
            self.query_stock_sql = "select a.security_code , a.exchange_code, " \
                                    "(select count(*) from tquant_calendar_info " \
                                    "where the_date > max(a.the_date)" \
                                    ") diff_day " \
                                    "from tquant_stock_day_kline a " \
                                    "group by a.security_code, a.exchange_code having diff_day > 0"
        else:
            self.query_stock_sql = "select a.security_code, a.exchange_code " \
                                   "from tquant_security_info a " \
                                   "where a.security_code not in " \
                                   "(select security_code from tquant_security_process_progress " \
                                   "where security_type = 'STOCK' " \
                                   "and business_type = '" + self.business_type + "' " \
                                   "and exchange_code = a.exchange_code " \
                                   "and process_progress = 1" \
                                   ") order by a.security_code, a.exchange_code asc "

        self.upsert = 'insert into tquant_stock_day_kline (security_code, the_date, exchange_code, ' \
                 'amount, vol, open, high, low, close) ' \
                 'values ({security_code}, {the_date}, {exchange_code}, ' \
                 '{amount}, {vol}, {open}, {high}, {low}, {close}) ' \
                 'on duplicate key update ' \
                 'amount=values(amount), vol=values(vol), open=values(open), ' \
                 'high=values(high), low=values(low), close=values(close) '

    def processing(self):
        """
        查询股票日K数据，并解析入库
        根据已有的股票代码，循环查询单个股票的日K数据，并解析入库
        :return:
        """
        print(datetime.datetime.now(), self.serviceName, 'is_increment', self.is_increment, 'processing start ... {}'.format(datetime.datetime.now()))
        getcontext().prec = 4
        try:
            # 需要处理的股票代码
            stock_tuple = self.dbService.query(self.query_stock_sql)
            print(datetime.datetime.now(), self.serviceName, 'is_increment', self.is_increment, 'processing stock_tuple:', stock_tuple)
            if stock_tuple:
                # 需要处理的股票代码进度计数
                data_add_up = 0
                # 需要处理的股票代码进度打印字符
                data_process_line = ''
                for stock_item in stock_tuple:
                    try:
                        # 股票代码
                        security_code = stock_item[0]
                        exchange_code = stock_item[1]
                        # 注释掉的这行是因为在测试的时候发现返回的数据有问题，
                        # 当 security_code == '000505' the_date='2010-01-04' 时，返回的数据为：
                        # amount: [ 39478241.  39478241.]vol: [ 5286272.  5286272.]open: [ 7.5  7.5]high: [ 7.65  7.65]low: [ 7.36  7.36]close: [ 7.44  7.44]
                        # 正常返回的数据为：
                        # amount: 37416387.0 vol: 4989934.0 open: 7.36 high: 7.69 low: 7.36 close: 7.48
                        # 所以为了处理这个不同类型的情况，做了判断和检测测试
                        # if security_code == '000505':
                        # 查询security_code近diff_day的股票全部日K数据
                        # 如果是增量线程，则查询增量的df数据
                        if self.is_increment:
                            diff_day = stock_item[2]
                            day_kline_df = tt.get_last_n_daybar(security_code, diff_day, 'bfq')
                        # 如果不是增量，则为全量，查询全部日K数据
                        else:
                            day_kline_df = tt.get_all_daybar(security_code, 'bfq')
                        data_add_up = self.process_day_kline_df(day_kline_df, security_code, exchange_code, self.serviceName, False, data_add_up)
                        # 批量(10)列表的处理进度打印
                        if data_add_up % 10 == 0:
                            data_process_line += '#'
                            processing = Decimal(data_add_up) / Decimal(len(stock_tuple)) * 100
                            print(datetime.datetime.now(), self.serviceName, 'is_increment', self.is_increment, 'processing data inner', 'stock_tuple size:', len(stock_tuple), 'processing ',
                                  data_process_line,
                                  str(processing) + '%')
                            # time.sleep(1)
                    except Exception:
                        data_add_up += 1
                        traceback.print_exc()
                # 最后一批增量列表的处理进度打印
                if data_add_up % 10 != 0:
                    data_process_line += '#'
                    processing = Decimal(data_add_up) / Decimal(len(stock_tuple)) * 100
                    print(datetime.datetime.now(), self.serviceName, 'is_increment', self.is_increment, 'processing data outer', 'stock_tuple size:', len(stock_tuple), 'processing ',
                          data_process_line,
                          str(processing) + '%')
                    print(datetime.datetime.now(), self.serviceName, 'is_increment', self.is_increment, 'processing data all done ########################################')
                    # time.sleep(1)
        except Exception:
            traceback.print_exc()
        print(datetime.datetime.now(), self.serviceName, 'is_increment', self.is_increment, 'processing thread end ...', datetime.datetime.now())


    def process_day_kline_df(self, day_kline_df, security_code, exchange_code, serviceName, is_increment, data_add_up):
        # 索引值为日期
        indexes_values = day_kline_df.index.values
        # 临时存储批量更新sql的列表
        upsert_sql_list = []
        # 需要处理的单只股票进度计数
        add_up = 0
        # 需要处理的单只股票进度打印字符
        process_line = ''
        # 循环处理security_code的股票日K数据
        for idx in indexes_values:
            # 解析股票日K数据（每行）
            upsert_sql = self.analysis_columns(day_kline_df, idx, security_code, exchange_code)
            # 批量(100)提交数据更新
            if len(upsert_sql_list) == 3000:
                self.dbService.insert_many(upsert_sql_list)
                process_line += '='
                upsert_sql_list = []
                upsert_sql_list.append(upsert_sql)
                processing = Decimal(add_up) / Decimal(len(indexes_values)) * 100
                print(datetime.datetime.now(), serviceName, 'is_increment', self.is_increment, 'processing data inner', security_code, 'day_kline_df size:',
                      len(indexes_values), 'processing ',
                      process_line,
                      str(processing) + '%')
                add_up += 1
                # 批量提交数据后当前线程休眠1秒
                # time.sleep(1)
            else:
                upsert_sql_list.append(upsert_sql)
                add_up += 1
        # 处理最后一批security_code的更新语句
        if len(upsert_sql_list) > 0:
            self.dbService.insert_many(upsert_sql_list)
            process_line += '='
            # 为了代码复用，增加一个是否增量标识，如果不是增量则更新进度记录，如果是增量则不做操作
            if is_increment != True:
                # 更新进度记录
                data_dict = {}
                data_dict['business_type'] = self.business_type
                data_dict['security_code'] = security_code
                data_dict['security_type'] = 'STOCK'
                data_dict['exchange_code'] = exchange_code
                data_dict['process_progress'] = 1
                self.dbService.upsert_tquant_security_process_progress(data_dict)

            processing = Decimal(add_up) / Decimal(len(indexes_values)) * 100
            print(datetime.datetime.now(), serviceName, 'is_increment', self.is_increment, 'processing data outer', security_code, 'day_kline_df size:',
                  len(indexes_values), 'processing ', process_line,
                  str(processing) + '%')
        print(datetime.datetime.now(), serviceName, 'is_increment', self.is_increment, 'processing data ', security_code,
              ' done =============================================')
        # time.sleep(1)

        data_add_up += 1
        return data_add_up

    def analysis_columns(self, day_kline, idx, security_code, exchange_code):
        """
        解析股票日K数据（每行）
        :param day_kline: 日K的DataFrame对象
        :param idx: day_kline的单行索引值，这里是日期值
        :param security_code: 证券代码，这里是股票代码
        :return: 返回值为设置完后的upsert_sql，即已经填充了解析后的值
        """
        value_dict = {}
        value_dict['security_code'] = security_code
        the_date = (str(idx))[0:10]
        value_dict['the_date'] = the_date
        value_dict['exchange_code'] = exchange_code

        amount = day_kline.at[idx, 'amount']
        # amount的类型为numpy.ndarray，是一个多维数组，可能包含多个值，其他的字段也是一样，测试的时候发现有异常抛出
        if isinstance(amount, numpy.ndarray) and amount.size > 1:
            amount = amount.tolist()[0]
        value_dict['amount'] = round(float(amount) / 10000, 4)

        vol = day_kline.at[idx, 'vol']
        if isinstance(vol, numpy.ndarray) and vol.size > 1:
            vol = vol.tolist()[0]
        value_dict['vol'] = round(float(vol) / 1000000, 4)

        open = day_kline.at[idx, 'open']
        if isinstance(open, numpy.ndarray) and open.size > 1:
            open = open.tolist()[0]
        value_dict['open'] = round(float(open), 2)

        high = day_kline.at[idx, 'high']
        if isinstance(high, numpy.ndarray) and high.size > 1:
            high = high.tolist()[0]
        value_dict['high'] = round(float(high), 2)

        low = day_kline.at[idx, 'low']
        if isinstance(low, numpy.ndarray) and low.size > 1:
            low = low.tolist()[0]
        value_dict['low'] = round(float(low), 2)

        close = day_kline.at[idx, 'close']
        if isinstance(close, numpy.ndarray) and close.size > 1:
            close = close.tolist()[0]
        value_dict['close'] = round(float(close), 2)

        upsert_sql = self.upsert.format(
            security_code="'" + value_dict['security_code'] + "'",
            the_date="'" + value_dict['the_date'] + "'",
            exchange_code="'" + value_dict['exchange_code'] + "'",
            amount=value_dict['amount'],
            vol=value_dict['vol'],
            open=value_dict['open'],
            high=value_dict['high'],
            low=value_dict['low'],
            close=value_dict['close']
        )

        return upsert_sql