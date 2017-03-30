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

        self.log_list = [self.get_clsss_name()]

        init_log_list = self.deepcopy_list(self.log_list)
        init_log_list.append(self.get_method_name())
        init_log_list.append('sleep seconds')
        init_log_list.append(sleep_seconds)
        init_log_list.append('one_time')
        init_log_list.append(one_time)
        self.logger.info(init_log_list)

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
        loop_log_list = self.deepcopy_list(self.log_list)
        loop_log_list.append(self.get_method_name())
        self.logger.info(loop_log_list)
        while True:
            self.processing(loop_log_list)
            if self.one_time:
                break
            time.sleep(self.sleep_seconds)

    def processing(self, loop_log_list):
        """
        根据已有的股票代码，循环查询单个股票的日K数据
        :return:
        """
        if loop_log_list is not None and len(loop_log_list) > 0:
            processing_log_list = self.deepcopy_list(loop_log_list)
        else:
            processing_log_list = self.deepcopy_list(self.log_list)
        processing_log_list.append(self.get_method_name())

        start_log_list = self.deepcopy_list(processing_log_list)
        start_log_list.append('【start】')
        self.logger.info(start_log_list)

        # 获取交易日表最大交易日日期，类型为date.datetime
        calendar_max_the_date = self.get_calendar_max_the_date()
        # 需要处理的股票代码
        result = self.dbService.query(self.query_stock_sql)
        self.processing_security_codes(processing_log_list, result, calendar_max_the_date, 'batch-0')

        end_log_list = self.deepcopy_list(processing_log_list)
        end_log_list.append('【end】')
        self.logger.info(end_log_list)

    def processing_security_codes(self, processing_log_list, tuple_security_codes, calendar_max_the_date, batch_name):
        """
        处理一组股票代码的日K数据
        :param list_security_codes: 
        :param calendar_max_the_date: 
        :return: 
        """
        if processing_log_list is not None and len(processing_log_list) > 0:
            security_codes_log_list = self.deepcopy_list(processing_log_list)
        else:
            security_codes_log_list = self.deepcopy_list(self.log_list)
        security_codes_log_list.append(batch_name)
            
        start_log_list = self.deepcopy_list(security_codes_log_list)
        start_log_list.append('tuple_security_codes size')
        start_log_list.append(len(tuple_security_codes))
        start_log_list.append('calendar_max_the_date')
        start_log_list.append(calendar_max_the_date)
        start_log_list.append('【start】')
        self.logger.info(start_log_list)
        try:

            if tuple_security_codes is not None and len(tuple_security_codes) > 0:
                len_result = len(tuple_security_codes)
                # 需要处理的股票代码进度计数
                add_up = 0
                # 需要处理的股票代码进度打印字符
                process_line = '#'
                for stock_item in tuple_security_codes:
                    security_code = None
                    exchange_code = None
                    # time.sleep(2)
                    try:
                        # 股票代码
                        security_code = stock_item[0]
                        exchange_code = stock_item[1]
                        # 根据security_code和exchange_code和ma查询日K已经处理的最大交易日
                        day_kline_max_the_date = self.get_day_kline_max_the_date(security_code, exchange_code)
                        recent_few_days = 0
                        if day_kline_max_the_date is not None and day_kline_max_the_date != '':
                            # 如果均线已经处理的最大交易日和交易日表的最大交易日相等，说明无需处理该均线数据，继续下一个处理
                            if calendar_max_the_date == day_kline_max_the_date:
                                warn_log_list = self.deepcopy_list(security_codes_log_list)
                                warn_log_list.append(security_code)
                                warn_log_list.append(exchange_code)
                                warn_log_list.append('day_kline_max_the_date')
                                warn_log_list.append(day_kline_max_the_date)
                                self.logger.warn(warn_log_list)
                                continue
                            # 根据day_kline_max_the_date已经处理的均线最大交易日，获取还需要最近几个交易日的日K数据
                            recent_few_days = self.get_calendar_recent_few_the_date(day_kline_max_the_date)
                            if recent_few_days == 0:
                                warn_log_list = self.deepcopy_list(security_codes_log_list)
                                warn_log_list.append(security_code)
                                warn_log_list.append(exchange_code)
                                warn_log_list.append('day_kline_max_the_date')
                                warn_log_list.append(day_kline_max_the_date)
                                warn_log_list.append('recent_few_days size')
                                warn_log_list.append(recent_few_days)
                                self.logger.warn(warn_log_list)
                                continue

                        self.processing_single_security_code(security_codes_log_list, security_code, exchange_code, recent_few_days)
                        # 批量(10)列表的处理进度打印
                        if add_up % 10 == 0:
                            process_line += '#'
                            processing = self.base_round(Decimal(add_up) / Decimal(len_result), 4) * 100
                            
                            batch_log_list = self.deepcopy_list(security_codes_log_list)
                            batch_log_list.append('inner')
                            batch_log_list.append(add_up)
                            batch_log_list.append(len_result)
                            batch_log_list.append(process_line)
                            batch_log_list.append(str(processing) + '%')
                            self.logger.info(batch_log_list)
                        add_up += 1
                    except Exception:
                        exc_type, exc_value, exc_traceback = sys.exc_info()
                        except_log_list = self.deepcopy_list(security_codes_log_list)
                        except_log_list.append('inner')
                        except_log_list.append(security_code)
                        except_log_list.append(exchange_code)
                        except_log_list.append(exc_type)
                        except_log_list.append(exc_value)
                        except_log_list.append(exc_traceback)
                        self.logger.exception(except_log_list)
                # 最后一批增量列表的处理进度打印
                if add_up % 10 != 0:
                    process_line += '#'
                    processing = self.base_round(Decimal(add_up) / Decimal(len_result), 4) * 100
                    
                    batch_log_list = self.deepcopy_list(security_codes_log_list)
                    batch_log_list.append('outer')
                    batch_log_list.append(add_up)
                    batch_log_list.append(len_result)
                    batch_log_list.append(process_line)
                    batch_log_list.append(str(processing) + '%')
                    self.logger.info(batch_log_list)
        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            except_log_list = self.deepcopy_list(security_codes_log_list)
            except_log_list.append('outer')
            except_log_list.append(exc_type)
            except_log_list.append(exc_value)
            except_log_list.append(exc_traceback)
            self.logger.exception(except_log_list)

        end_log_list = self.deepcopy_list(security_codes_log_list)
        end_log_list.append('tuple_security_codes size')
        end_log_list.append(len(tuple_security_codes))
        end_log_list.append('calendar_max_the_date')
        end_log_list.append(calendar_max_the_date)
        end_log_list.append('【end】')
        self.logger.info(end_log_list)

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

    def processing_single_security_code(self, security_codes_log_list, security_code, exchange_code, recent_few_days):
        """
        处理增量单只股票的日K数据，如果recent_few_days==0，则处理全量数据
        :param security_code: 股票代码
        :param exchange_code: 交易所
        :param recent_few_days: 需要处理近几日的数字
        :param add_up: 处理进度增量标示
        :return:
        """
        if security_codes_log_list is not None and len(security_codes_log_list) > 0:
            single_log_list = self.deepcopy_list(security_codes_log_list)
        else:
            single_log_list = self.deepcopy_list(self.log_list)
        single_log_list.append(self.get_method_name())
        single_log_list.append(security_code)
        single_log_list.append(exchange_code)
        single_log_list.append('recent_few_days')
        single_log_list.append(recent_few_days)
        
        start_log_list = self.deepcopy_list(single_log_list)
        start_log_list.append('【start】')
        self.logger.info(start_log_list)
        # 注释掉的这行是因为在测试的时候发现返回的数据有问题，
        # 当 security_code == '000505' the_date='2010-01-04' 时，返回的数据为：
        # amount: [ 39478241.  39478241.]vol: [ 5286272.  5286272.]open: [ 7.5  7.5]high: [ 7.65  7.65]
        # low: [ 7.36  7.36]close: [ 7.44  7.44]
        # 正常返回的数据为：
        # amount: 37416387.0 vol: 4989934.0 open: 7.36 high: 7.69 low: 7.36 close: 7.48
        # 所以为了处理这个不同类型的情况，做了判断和检测测试
        # if security_code == '000505':
        try:
            if recent_few_days == 0:
                result = tt.get_all_daybar(security_code, 'bfq')
            else:
                result = tt.get_last_n_daybar(security_code, recent_few_days, 'bfq')
            if result is not None and result.empty is False:
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
                    len_indexes = len(indexes_values)
                    for idx in indexes_values:
                        # 解析股票日K数据（每行）
                        # 解析每行的返回值格式为list [the_date, amount, vol, open, high, low, close]
                        list_data = self.analysis_columns(single_log_list, result, idx)
                        if list_data is not None:
                            upsert_sql = self.upsert.format(
                                security_code=self.quotes_surround(security_code),
                                the_date=self.quotes_surround(list_data[0]),
                                exchange_code=self.quotes_surround(exchange_code),
                                amount=list_data[1],
                                vol=list_data[2],
                                open=list_data[3],
                                high=list_data[4],
                                low=list_data[5],
                                close=list_data[6]
                            )
                        else:
                            upsert_sql = None
                            warn_log_list = self.deepcopy_list(single_log_list)
                            warn_log_list.append('analysis_columns list_data is None')
                            self.logger.warn(warn_log_list)

                            continue
                        # 批量(100)提交数据更新
                        if len(upsert_sql_list) == 3000:
                            self.dbService.insert_many(upsert_sql_list)
                            process_line += '='
                            upsert_sql_list = []
                            if upsert_sql is not None:
                                upsert_sql_list.append(upsert_sql)
                            processing = self.base_round(Decimal(add_up) / Decimal(len_indexes), 4) * 100
                            batch_log_list = self.deepcopy_list(single_log_list)
                            batch_log_list.append('inner')
                            batch_log_list.append(add_up)
                            batch_log_list.append(len_indexes)
                            batch_log_list.append(process_line)
                            batch_log_list.append(str(processing) + '%')
                            self.logger.info(batch_log_list)
                        else:
                            upsert_sql_list.append(upsert_sql)
                        add_up += 1
                    # 处理最后一批security_code的更新语句
                    if len(upsert_sql_list) > 0:
                        self.dbService.insert_many(upsert_sql_list)
                        process_line += '='
                        processing = self.base_round(Decimal(add_up) / Decimal(len(indexes_values)), 4) * 100

                        batch_log_list = self.deepcopy_list(single_log_list)
                        batch_log_list.append('outer')
                        batch_log_list.append(add_up)
                        batch_log_list.append(len_indexes)
                        batch_log_list.append(process_line)
                        batch_log_list.append(str(processing) + '%')
                        self.logger.info(batch_log_list)
                else:
                    warn_log_list = self.deepcopy_list(single_log_list)
                    warn_log_list.append('recent_few_days')
                    warn_log_list.append(recent_few_days)
                    warn_log_list.append('result index.values is None')
                    self.logger.warn(warn_log_list)
            else:
                warn_log_list = self.deepcopy_list(single_log_list)
                warn_log_list.append('recent_few_days')
                warn_log_list.append(recent_few_days)
                warn_log_list.append('result is None')
                self.logger.warn(warn_log_list)

        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            except_log_list = self.deepcopy_list(single_log_list)
            except_log_list.append(exc_type)
            except_log_list.append(exc_value)
            except_log_list.append(exc_traceback)
            self.logger.exception(except_log_list)

        end_log_list = self.deepcopy_list(single_log_list)
        end_log_list.append('【end】')
        self.logger.info(end_log_list)

    def get_calendar_recent_few_the_date(self, day_kline_max_the_date):
        """
        查询股票日K数据还需要多少日，返回数字为需要查询近多少天
        :param day_kline_max_the_date:
        :return:
        """
        sql = "select count(*) from tquant_calendar_info where the_date > {day_kline_max_the_date} "
        recent_few_days = self.dbService.query(sql.format(day_kline_max_the_date=self.quotes_surround(day_kline_max_the_date.strftime('%Y-%m-%d'))))
        if recent_few_days:
            return recent_few_days[0][0]
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

    def analysis_columns(self, single_log_list, day_kline, idx):
        """
        解析股票日K数据（每行）
        :param day_kline: 日K的DataFrame对象
        :param idx: day_kline的单行索引值，这里是日期值
        :param security_code: 证券代码，这里是股票代码
        :return: 返回值为设置完后的upsert_sql，即已经填充了解析后的值
        """
        try:
            the_date = (str(idx))[0:10]

            amount = day_kline.at[idx, 'amount']
            # amount的类型为numpy.ndarray，是一个多维数组，可能包含多个值，其他的字段也是一样，测试的时候发现有异常抛出
            if isinstance(amount, numpy.ndarray) and amount.size > 1:
                amount = amount.tolist()[0]
            amount = self.base_round(amount / 10000, 4)

            vol = day_kline.at[idx, 'vol']
            if isinstance(vol, numpy.ndarray) and vol.size > 1:
                vol = vol.tolist()[0]
            vol = self.base_round(vol / 1000000, 4)

            open = day_kline.at[idx, 'open']
            if isinstance(open, numpy.ndarray) and open.size > 1:
                open = open.tolist()[0]
            open = self.base_round(open, 4)

            high = day_kline.at[idx, 'high']
            if isinstance(high, numpy.ndarray) and high.size > 1:
                high = high.tolist()[0]
            high = self.base_round(high, 4)

            low = day_kline.at[idx, 'low']
            if isinstance(low, numpy.ndarray) and low.size > 1:
                low = low.tolist()[0]
            low = self.base_round(low, 4)

            close = day_kline.at[idx, 'close']
            if isinstance(close, numpy.ndarray) and close.size > 1:
                close = close.tolist()[0]
            close = self.base_round(close, 4)

            return [the_date, amount, vol, open, high, low, close]
        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            except_log_list = self.deepcopy_list(single_log_list)
            except_log_list.append(exc_type)
            except_log_list.append(exc_value)
            except_log_list.append(exc_traceback)
            self.logger.exception(except_log_list)
            return None