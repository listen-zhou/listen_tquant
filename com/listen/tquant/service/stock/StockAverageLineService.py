# coding: utf-8

import decimal
from decimal import *
context = decimal.getcontext()
context.rounding = decimal.ROUND_05UP

import sys

from com.listen.tquant.service.BaseService import BaseService

import time


class StockAverageLineService(BaseService):
    """
    股票均线数据处理服务
    """
    def __init__(self, dbService, ma, logger):
        super(StockAverageLineService, self).__init__(logger)
        self.ma = ma
        self.dbService = dbService
        self.base_info('{0[0]} ...', [self.get_current_method_name()])

        self.query_stock_sql = "select a.security_code, a.exchange_code " \
                               "from tquant_security_info a " \
                               "where a.security_type = 'STOCK'"

        self.upsert = 'insert into tquant_stock_average_line (security_code, the_date, exchange_code, ' \
                      'ma, ' \
                      'close, previous_close, close_change_percent,' \
                      'amount, previous_amount, amount_change_percent,' \
                      'vol, previous_vol, vol_change_percent) ' \
                      'values ({security_code}, {the_date}, {exchange_code}, ' \
                      '{ma}, ' \
                      '{close}, {previous_close}, {close_change_percent}, ' \
                      '{amount}, {previous_amount}, {amount_change_percent}, ' \
                      '{vol}, {previous_vol}, {vol_change_percent}) ' \
                      'on duplicate key update ' \
                      'close=values(close), previous_close=values(previous_close), close_change_percent=values(close_change_percent), ' \
                      'amount=values(amount), previous_amount=values(previous_amount), amount_change_percent=values(amount_change_percent), ' \
                      'vol=values(vol), previous_vol=values(previous_vol), vol_change_percent=values(vol_change_percent)'

        self.upsert_none = 'insert into tquant_stock_average_line (security_code, the_date, exchange_code, ' \
                           'ma, close) ' \
                           'values ({security_code}, {the_date}, {exchange_code}, ' \
                           '{ma}, ' \
                           '{close}, {amount}, {vol}) ' \
                           'on duplicate key update ' \
                           'close=values(close), amount=values(amount), vol=values(vol) '

    def processing(self):
        """
        根据已有的股票代码，循环查询单个股票的日K数据
        :return:
        """
        self.base_info('{0[0]} 【start】...', [self.get_current_method_name()])
        try:
            # 获取交易日表最大交易日日期，类型为date.datetime
            calendar_max_the_date = self.get_calendar_max_the_date()
            # 需要处理的股票代码，查询股票基本信息表 security_code, exchange_code
            result = self.dbService.query(self.query_stock_sql)
            if result:
                len_result = len(result)
                # 需要处理的股票代码进度计数
                data_add_up = 0
                # 需要处理的股票代码进度打印字符
                process_line = ''
                for stock_item in result:
                    data_add_up += 1
                    # 股票代码
                    security_code = stock_item[0]
                    exchange_code = stock_item[1]
                    # 根据security_code和exchange_code和ma查询ma均线已经处理的最大交易日
                    average_line_max_the_date = self.get_average_line_max_the_date(security_code, exchange_code)
                    # 如果均线已经处理的最大交易日和交易日表的最大交易日相等，说明无需处理该均线数据，继续下一个处理
                    if calendar_max_the_date == average_line_max_the_date:
                        self.base_warn('{0[0]} {0[1]} {0[2]} {0[3]} calendar_max_the_date {0[4]} == average_line_max_the_date {0[5]}',
                                       [self.get_current_method_name(), self.ma, security_code, exchange_code, calendar_max_the_date,
                                        average_line_max_the_date])
                        continue
                    # 根据average_line_max_the_date已经处理的均线最大交易日，获取递减ma个交易日后的交易日
                    decline_ma_the_date = self.get_calendar_decline_ma_the_date(average_line_max_the_date)
                    self.processing_single_security_code(security_code, exchange_code, decline_ma_the_date)
                    # 批量(10)列表的处理进度打印
                    if data_add_up % 10 == 0:
                        if data_add_up % 100 == 0:
                            process_line += '#'
                        processing = round(Decimal(data_add_up) / Decimal(len_result), 4) * 100
                        self.base_info('{0[0]} {0[1]} {0[2]} {0[3]} {0[4]} {0[5]}%...',
                                       [self.get_current_method_name(), self.ma, 'inner', len_result, process_line,
                                        processing])
                        # time.sleep(1)

                # 最后一批增量列表的处理进度打印
                if data_add_up % 10 != 0:
                    if data_add_up % 100 == 0:
                        process_line += '#'
                    processing = round(Decimal(data_add_up) / Decimal(len_result), 4) * 100
                    self.base_info('{0[0]} {0[1]} {0[2]} {0[3]} {0[4]} {0[5]}%',
                                   [self.get_current_method_name(), self.ma, 'outer', len_result, process_line,
                                    processing])
                    # time.sleep(1)
        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.base_error('{0[0]} {0[1]} {0[2]} {0[3]} ',
                            [self.get_current_method_name(), exc_type, exc_value, exc_traceback])
        self.base_info('{0[0]} 【end】', [self.get_current_method_name()])


    def process_day_kline_tuple(self, result, security_code, exchange_code):
        self.base_debug('{0[0]} {0[1]} {0[2]}【start】...',
                       [self.get_current_method_name(), security_code, exchange_code])

        self.base_debug('{0[0]} {0[1]} {0[2]}【end】',
                       [self.get_current_method_name(), security_code, exchange_code])

    def get_previous_data(self, security_code, exchange_code, the_date):
        sql = "select price, amount, vol from tquant_stock_average_line where security_code = {security_code} " \
              "and exchange_code = {exchange_code} and ma = {ma} and the_date < {the_date} " \
              "order by the_date desc limit 1".format(security_code="'" + security_code + "'",
                                                      exchange_code="'" + exchange_code + "'",
                                                      ma=self.ma,
                                                      the_date="'" + the_date.strftime('%Y-%m-%d') + "'")
        previous_data = self.dbService.query(sql)
        return previous_data

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

    def get_average_line_max_the_date(self, security_code, exchange_code):
        sql = "select max(the_date) max_the_date from tquant_stock_average_line " \
              "where security_code = {security_code} " \
              "and exchange_code = {exchange_code} " \
              "and ma = {ma}"
        the_date = self.dbService.query(sql.format(security_code="'"+security_code+"'",
                                                   exchange_code="'"+exchange_code+"'",
                                                   ma=self.ma))
        if the_date:
            max_the_date = the_date[0][0]
            return max_the_date
        return None

    def get_calendar_decline_ma_the_date(self, average_line_max_the_date):
        if average_line_max_the_date != None and average_line_max_the_date != '':
            sql = "select min(the_date) from (select the_date from tquant_calendar_info " \
                  "where the_date <= {average_line_max_the_date} " \
                  "order by the_date desc limit {ma}) a"
        else:
            sql = "select min(the_date) from tquant_calendar_info"
        the_date = self.dbService.query(sql.format(average_line_max_the_date="'"+str(average_line_max_the_date)+"'",
                                                   ma=self.ma))
        if the_date != None and the_date != '':
            decline_ma_the_date = the_date[0][0]
            return decline_ma_the_date
        return None

    def processing_single_security_code(self, security_code, exchange_code, decline_ma_the_date):
        """
        处理单只股票的均线数据
        :param security_code: 股票代码
        :param exchange_code: 交易所代码
        :param data_add_up: 针对批量处理股票代码时传入的进度参数
        :param decline_ma_the_date: 根据已经处理均线数据的最大交易日往前递减ma个交易日后的交易日，如果是单只股票执行，则可设置为1970-01-01日期
        :return: 返回批量处理时传入的进度累加值data_add_up
        """
        self.base_info('{0[0]} {0[1]} {0[2]} {0[3]} decline_ma_the_date {0[4]} 【start】...',
                       [self.get_current_method_name(), self.ma, security_code, exchange_code, decline_ma_the_date])
        try:
            sql = "select the_date, close, amount, vol " \
                  "from tquant_stock_day_kline " \
                  "where security_code = {security_code} " \
                  "and exchange_code = {exchange_code} "
            max_the_date = None
            if decline_ma_the_date != None:
                sql += "and the_date >= {max_the_date} "
                max_the_date = decline_ma_the_date.strftime('%Y-%m-%d')
            sql += "order by the_date asc "
            sql = sql.format(security_code="'" + security_code + "'",
                             exchange_code="'" + exchange_code + "'",
                             max_the_date="'" + max_the_date + "'")
            result = self.dbService.query(sql)

            # 开始解析股票日K数据, the_date, close
            # 临时存储批量更新sql的列表
            upsert_sql_list = []
            # 需要处理的单只股票进度计数
            add_up = 0
            # 需要处理的单只股票进度打印字符
            process_line = ''
            # 循环处理security_code的股票日K数据
            i = 0
            # 由于是批量提交数据，所以在查询前一日均价时，有可能还未提交，
            # 所以只在第一次的时候查询，其他的情况用前一次计算的均价作为前一日均价
            # is_first就是是否第一次需要查询的标识
            is_first = True

            previous_close = None
            previous_amount = None
            previous_vol = None
            # 前一日均值
            previous_data = None
            len_result = len(result)
            while i < len_result:
                add_up += 1
                # 根据ma切片, 切片下标索引为i+self.ma
                section_idx = i + self.ma
                # 如果切片的下标是元祖的最后一个元素，则退出，因为已经处理完毕
                if section_idx > (len_result):
                    break
                temp_line_tuple = result[i:(i + self.ma)]
                # temp_line_tuple中的数据为the_date, close
                if temp_line_tuple and self.ma == len(temp_line_tuple):
                    # 处理数据的交易日为切片的最后一个元素的the_date
                    the_date = temp_line_tuple[len(temp_line_tuple) - 1][0]
                    temp_items = [item for item in temp_line_tuple[0:]]
                    close_list = [close for close in [item[1] for item in temp_items]]
                    amount_list = [amount for amount in [item[2] for item in temp_items]]
                    vol_list = [vol for vol in [item[3] for item in temp_items]]
                    average_close = self.average(close_list)
                    average_amount = self.average(amount_list)
                    average_vol = self.average(vol_list)
                    if is_first:
                        previous_data = self.get_previous_data(security_code, exchange_code, the_date)
                        # 第一次查询过后，无论查询是否有数据，都不再查询
                        is_first = False
                        if previous_data != None:
                            previous_close = previous_data[0][0]
                            previous_amount = previous_data[0][1]
                            previous_vol = previous_data[0][2]
                    if previous_close != None and previous_close != Decimal(0):
                        close_change_percent = round((average_close - previous_close) / previous_close) * 100
                    else:
                        close_change_percent = None
                    if previous_amount != None and previous_amount != Decimal(0):
                        amount_change_percent = round((average_amount - previous_amount) / previous_amount) * 100
                    else:
                        amount_change_percent = None
                    if previous_vol != None and previous_vol != Decimal(0):
                        vol_change_percent = round((average_vol - previous_vol) / previous_vol) * 100
                    else:
                        vol_change_percent = None
                    if close_change_percent == None and amount_change_percent == None and vol_change_percent == None:
                        upsert_sql = self.upsert_none
                    else:
                        upsert_sql = self.upsert
                    upsert_sql = upsert_sql.format(security_code="'" + security_code + "'",
                                                   the_date="'" + the_date.strftime('%Y-%m-%d') + "'",
                                                   exchange_code="'" + exchange_code + "'",
                                                   ma=self.ma,
                                                   close=average_close,
                                                   previous_close=previous_close,
                                                   close_change_percent=close_change_percent,
                                                   amount=average_amount,
                                                   previous_amount=previous_amount,
                                                   amount_change_percent=amount_change_percent,
                                                   vol=average_vol,
                                                   previous_vol=previous_vol,
                                                   vol_change_percent=vol_change_percent
                                                   )
                    # 批量(100)提交数据更新
                    if len(upsert_sql_list) == 3000:
                        self.dbService.insert_many(upsert_sql_list)
                        process_line += '='
                        upsert_sql_list = []
                        upsert_sql_list.append(upsert_sql)
                        if len_result == self.ma:
                            processing = 1.0
                        else:
                            processing = round(Decimal(add_up) / Decimal(len_result - self.ma), 4) * 100
                        self.base_debug('{0[0]} {0[1]} {0[2]} {0[3]} {0[4]}%...',
                                        [self.get_current_method_name(), 'inner', len_result, process_line,
                                         processing])
                        # 批量提交数据后当前线程休眠1秒
                        # time.sleep(1)
                    else:
                        upsert_sql_list.append(upsert_sql)
                i += 1
                # 设置下一个切片的前一日均价为当前切片的均价，以备下一个切片计算涨跌幅使用
                previous_close = average_close
                previous_amount = average_amount
                previous_vol = average_vol

            # 处理最后一批security_code的更新语句
            if len(upsert_sql_list) > 0:
                self.dbService.insert_many(upsert_sql_list)
                process_line += '='
                if len_result == self.ma:
                    processing = 1.0
                else:
                    processing = round(Decimal(add_up) / Decimal(len_result - self.ma), 4) * 100
                self.base_debug('{0[0]} {0[1]} {0[2]} {0[3]} {0[4]}%',
                                [self.get_current_method_name(), 'outer', len_result, process_line, processing])
        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.base_error('{0[0]} {0[1]} {0[2]} {0[3]} ',
                            [self.get_current_method_name(), exc_type, exc_value, exc_traceback])
        self.base_info('{0[0]} {0[1]} {0[2]} {0[3]} decline_ma_the_date {0[4]} 【end】',
                       [self.get_current_method_name(), self.ma, security_code, exchange_code, decline_ma_the_date])
