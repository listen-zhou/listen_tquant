# coding: utf-8
from decimal import *
import decimal
context = decimal.getcontext()
context.rounding = decimal.ROUND_05UP

import numpy
import tquant as tt
import time
import sys


from com.listen.tquant.service.BaseService import BaseService


class StockDayKlineService(BaseService):
    """
    股票日K数据处理服务
    """
    def __init__(self, dbService, logger, sleep_seconds, one_time):
        super(StockDayKlineService, self).__init__(logger)
        self.dbService = dbService
        self.sleep_seconds = sleep_seconds
        self.one_time = one_time
        self.base_info('{0[0]} ...', [self.get_current_method_name()])
        self.query_stock_sql = "select a.security_code, a.exchange_code " \
                               "from tquant_security_info a " \
                               "where a.security_type = 'STOCK'"

        self.upsert = 'insert into tquant_stock_day_kline (security_code, the_date, exchange_code, ' \
                 'amount, vol, open, high, low, close) ' \
                 'values ({security_code}, {the_date}, {exchange_code}, ' \
                 '{amount}, {vol}, {open}, {high}, {low}, {close}) ' \
                 'on duplicate key update ' \
                 'amount=values(amount), vol=values(vol), open=values(open), ' \
                 'high=values(high), low=values(low), close=values(close) '

    def loop(self):
        while True:
            self.processing()
            if self.one_time:
                break
            time.sleep(self.sleep_seconds)

    def processing(self):
        """
        根据已有的股票代码，循环查询单个股票的日K数据
        :return:
        """
        self.base_info('{0[0]} 【start】...', [self.get_current_method_name()])
        # 获取交易日表最大交易日日期，类型为date.datetime
        calendar_max_the_date = self.get_calendar_max_the_date()
        # 需要处理的股票代码
        result = self.dbService.query(self.query_stock_sql)
        self.processing_security_codes(result, calendar_max_the_date, 'batch-0')
        self.base_info('{0[0]} 【end】', [self.get_current_method_name()])

    def processing_security_codes(self, tuple_security_codes, calendar_max_the_date, batch_number):
        """
        处理一组股票代码的日K数据
        :param list_security_codes: 
        :param calendar_max_the_date: 
        :return: 
        """
        self.base_info('{0[0]} {0[1]}【start】...', [self.get_current_method_name(), batch_number])
        try:

            if tuple_security_codes != None and len(tuple_security_codes) > 0:
                len_result = len(tuple_security_codes)
                # 需要处理的股票代码进度计数
                data_add_up = 0
                # 需要处理的股票代码进度打印字符
                process_line = '#'
                security_code = None
                exchange_code = None
                for stock_item in tuple_security_codes:
                    time.sleep(2)
                    try:
                        data_add_up += 1
                        # 股票代码
                        security_code = stock_item[0]
                        exchange_code = stock_item[1]
                        # 根据security_code和exchange_code和ma查询日K已经处理的最大交易日
                        day_kline_max_the_date = self.get_day_kline_max_the_date(security_code, exchange_code)
                        recent_few_the_date = 0
                        if day_kline_max_the_date != None and day_kline_max_the_date != '':
                            # 如果均线已经处理的最大交易日和交易日表的最大交易日相等，说明无需处理该均线数据，继续下一个处理
                            if calendar_max_the_date == day_kline_max_the_date:
                                self.base_warn(
                                    '{0[0]} {0[1]} {0[2]} {0[3]} calendar_max_the_date {0[4]} == day_kline_max_the_date {0[5]}',
                                    [self.get_current_method_name(), batch_number, security_code, exchange_code,
                                     calendar_max_the_date, day_kline_max_the_date])
                                continue
                            # 根据day_kline_max_the_date已经处理的均线最大交易日，获取还需要最近几个交易日的日K数据
                            recent_few_the_date = self.get_calendar_recent_few_the_date(day_kline_max_the_date)
                            if recent_few_the_date == 0:
                                self.base_warn(
                                    '{0[0]} {0[1]} {0[2]} {0[3]} calendar_max_the_date {0[4]} day_kline_max_the_date {0[5]} recent_few_the_date {0[6]}',
                                    [self.get_current_method_name(), batch_number, security_code, exchange_code,
                                     calendar_max_the_date, day_kline_max_the_date, recent_few_the_date])
                                continue

                        self.processing_single_security_code(security_code, exchange_code, recent_few_the_date, batch_number)

                        # 批量(10)列表的处理进度打印
                        if data_add_up % 10 == 0:
                            process_line += '#'
                            processing = self.base_round(Decimal(data_add_up) / Decimal(len_result), 4) * 100
                            self.base_info('{0[0]} {0[1]} {0[2]} {0[3]} {0[4]} {0[5]} {0[6]}%...',
                                           [self.get_current_method_name(), batch_number, 'inner', data_add_up, len_result,
                                            process_line,
                                            processing])
                    except Exception:
                        exc_type, exc_value, exc_traceback = sys.exc_info()
                        self.base_exception('{0[0]} {0[1]} {0[2]} {0[3]} {0[4]} {0[5]} {0[6]} {0[7]} {0[8]}',
                                            [self.get_current_method_name(), batch_number, 'inner', security_code, exchange_code,
                                             recent_few_the_date,
                                             exc_type, exc_value, exc_traceback])
                # 最后一批增量列表的处理进度打印
                if data_add_up % 10 != 0:
                    process_line += '#'
                    processing = self.base_round(Decimal(data_add_up) / Decimal(len_result), 4) * 100
                    self.base_info('{0[0]} {0[1]} {0[2]} {0[3]} {0[4]} {0[5]} {0[6]}%',
                                   [self.get_current_method_name(), batch_number, 'outer', data_add_up, len_result, process_line,
                                    processing])
        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.base_exception('{0[0]} {0[1]} {0[2]} {0[3]} {0[4]} {0[5]} {0[6]}',
                                [self.get_current_method_name(), batch_number, 'outer',
                                 recent_few_the_date,
                                 exc_type, exc_value, exc_traceback])
        self.base_info('{0[0]} {0[1]} 【end】', [self.get_current_method_name(), batch_number])

    def get_calendar_max_the_date(self):
        """
        查询交易日表中最大交易日日期
        :return:
        """
        sql = "select max(the_date) max_the_date from tquant_calendar_info"
        the_date = self.dbService.query(sql)
        if the_date:
            max_the_date = the_date[0][0]
            return max_the_date
        return None

    def processing_single_security_code(self, security_code, exchange_code, recent_few_the_date, batch_number):
        """
        处理增量单只股票的日K数据，如果recent_few_the_date==0，则处理全量数据
        :param security_code: 股票代码
        :param exchange_code: 交易所
        :param recent_few_the_date: 需要处理近几日的数字
        :param data_add_up: 处理进度增量标示
        :return:
        """
        self.base_debug('{0[0]} {0[1]} {0[2]} {0[3]}【start】...',
                       [self.get_current_method_name(), batch_number, security_code, exchange_code])
        # 注释掉的这行是因为在测试的时候发现返回的数据有问题，
        # 当 security_code == '000505' the_date='2010-01-04' 时，返回的数据为：
        # amount: [ 39478241.  39478241.]vol: [ 5286272.  5286272.]open: [ 7.5  7.5]high: [ 7.65  7.65]low: [ 7.36  7.36]close: [ 7.44  7.44]
        # 正常返回的数据为：
        # amount: 37416387.0 vol: 4989934.0 open: 7.36 high: 7.69 low: 7.36 close: 7.48
        # 所以为了处理这个不同类型的情况，做了判断和检测测试
        # if security_code == '000505':
        try:
            if recent_few_the_date == 0:
                result = tt.get_all_daybar(security_code, 'bfq')
            else:
                result = tt.get_last_n_daybar(security_code, recent_few_the_date, 'bfq')
            if result is not None and result.empty == False:
                # 索引值为日期
                indexes_values = result.index.values
                # 临时存储批量更新sql的列表
                upsert_sql_list = []
                # 需要处理的单只股票进度计数
                add_up = 0
                # 需要处理的单只股票进度打印字符
                process_line = '='
                # 循环处理security_code的股票日K数据
                if indexes_values is not None:
                    for idx in indexes_values:
                        add_up += 1
                        # 解析股票日K数据（每行）
                        upsert_sql = self.analysis_columns(result, idx, security_code, exchange_code)
                        # 批量(100)提交数据更新
                        if len(upsert_sql_list) == 3000:
                            self.dbService.insert_many(upsert_sql_list)
                            process_line += '='
                            upsert_sql_list = []
                            if upsert_sql != None:
                                upsert_sql_list.append(upsert_sql)
                            processing = self.base_round(Decimal(add_up) / Decimal(len(indexes_values)), 4) * 100
                            self.base_debug('{0[0]} {0[1]} {0[2]} {0[3]} {0[4]} {0[5]} {0[6]} {0[7]}%...',
                                            [self.get_current_method_name(), batch_number, 'inner', security_code, exchange_code,
                                             len(result), process_line,
                                             processing])
                        else:
                            upsert_sql_list.append(upsert_sql)
                    # 处理最后一批security_code的更新语句
                    if len(upsert_sql_list) > 0:
                        self.dbService.insert_many(upsert_sql_list)
                        process_line += '='
                        processing = self.base_round(Decimal(add_up) / Decimal(len(indexes_values)), 4) * 100
                        self.base_debug('{0[0]} {0[1]} {0[2]} {0[3]} {0[4]} {0[5]} {0[6]} {0[7]}%...',
                                        [self.get_current_method_name(), batch_number, 'outer', security_code,
                                         exchange_code,
                                         len(result), process_line,
                                         processing])
                else:
                    self.base_warn('{0[0]}, {0[1]}, {0[2]}, {0[3]} {0[4]} result index.values is None',
                                   [self.get_current_method_name(), batch_number, security_code, exchange_code, recent_few_the_date])
            else:
                self.base_warn('{0[0]}, {0[1]}, {0[2]}, {0[3]} {0[4]} result is None',
                               [self.get_current_method_name(), batch_number, security_code, exchange_code, recent_few_the_date])
        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.base_exception('{0[0]} {0[1]} {0[2]} {0[3]} {0[4]} {0[5]} {0[6]} {0[7]}',
                            [self.get_current_method_name(), batch_number, security_code, exchange_code, recent_few_the_date,
                             exc_type, exc_value, exc_traceback])
        self.base_debug('{0[0]} {0[1]} {0[2]} {0[3]} 【end】',
                       [self.get_current_method_name(), batch_number, security_code, exchange_code])

    def get_calendar_recent_few_the_date(self, day_kline_max_the_date):
        """
        查询股票日K数据还需要多少日，返回数字为需要查询近多少天
        :param day_kline_max_the_date:
        :return:
        """
        sql = "select count(*) from tquant_calendar_info where the_date > {day_kline_max_the_date} "
        recent_few_the_date = self.dbService.query(sql.format(day_kline_max_the_date=self.quotes_surround(day_kline_max_the_date.strftime('%Y-%m-%d'))))
        if recent_few_the_date:
            return recent_few_the_date[0][0]
        else:
            return 0

    def get_day_kline_max_the_date(self, security_code, exchange_code):
        """
        查询已经处理的日K数据的最大交易日
        :param security_code:
        :param exchange_code:
        :return:
        """
        sql = "select max(the_date) max_the_date from tquant_stock_day_kline " \
              "where security_code = {security_code} " \
              "and exchange_code = {exchange_code} "
        the_date = self.dbService.query(sql.format(security_code=self.quotes_surround(security_code),
                                                   exchange_code=self.quotes_surround(exchange_code)
                                                   ))
        if the_date:
            max_the_date = the_date[0][0]
            return max_the_date
        return None

    def analysis_columns(self, day_kline, idx, security_code, exchange_code):
        """
        解析股票日K数据（每行）
        :param day_kline: 日K的DataFrame对象
        :param idx: day_kline的单行索引值，这里是日期值
        :param security_code: 证券代码，这里是股票代码
        :return: 返回值为设置完后的upsert_sql，即已经填充了解析后的值
        """
        try:
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
                security_code=self.quotes_surround(value_dict['security_code']),
                the_date=self.quotes_surround(value_dict['the_date']),
                exchange_code=self.quotes_surround(value_dict['exchange_code']),
                amount=value_dict['amount'],
                vol=value_dict['vol'],
                open=value_dict['open'],
                high=value_dict['high'],
                low=value_dict['low'],
                close=value_dict['close']
            )
            return upsert_sql
        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.base_exception('{0[0]} {0[1]} {0[2]} {0[3]} {0[4]} {0[5]} ',
                            [self.get_current_method_name(), security_code, exchange_code,
                             exc_type, exc_value, exc_traceback])
            return None