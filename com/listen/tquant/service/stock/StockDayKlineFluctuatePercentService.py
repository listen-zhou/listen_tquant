# coding: utf-8
import traceback
from decimal import *
import types

import numpy
import tquant as tt
import datetime
import time


class StockDayKlineFluctuatePercentService():
    """
    股票日K数据涨跌幅处理服务
    """
    def __init__(self, dbService):
        self.serviceName = 'StockDayKlineFluctuatePercentService'
        self.dbService = dbService
        print(datetime.datetime.now(), self.serviceName, 'init ...', datetime.datetime.now())
        self.query_stock_sql = "select a.security_code, a.exchange_code " \
                               "from tquant_security_info a " \
                               "where a.security_type = 'STOCK'"

        self.upsert = 'insert into tquant_stock_day_kline (security_code, the_date, exchange_code, ' \
                 'previous_close, fluctuate_percent) ' \
                 'values ({security_code}, {the_date}, {exchange_code}, ' \
                 '{previous_close}, {fluctuate_percent}) ' \
                 'on duplicate key update ' \
                 'previous_close=values(previous_close), fluctuate_percent=values(fluctuate_percent) '

    def processing(self):
        """
        查询股票日K数据，处理涨跌幅，并入库
        根据已有的股票代码，循环查询单个股票的日K数据
        :return:
        """
        print(datetime.datetime.now(), self.serviceName, 'processing start ... {}'.format(datetime.datetime.now()))
        try:
            # 需要处理的股票代码
            stock_tuple = self.dbService.query(self.query_stock_sql)
            print(datetime.datetime.now(), self.serviceName, 'processing stock_tuple:', stock_tuple)
            if stock_tuple:
                stock_tuple_len = len(stock_tuple)
                # 需要处理的股票代码进度计数
                data_add_up = 0
                # 需要处理的股票代码进度打印字符
                data_process_line = ''
                for stock_item in stock_tuple:
                    try:
                        # 股票代码
                        security_code = stock_item[0]
                        exchange_code = stock_item[1]
                        # 根据security_code和exchange_code日K已经处理的最大交易日
                        day_kline_max_the_date = self.get_day_kline_max_the_date(security_code, exchange_code)
                        # print('day_kline_max_the_date', day_kline_max_the_date)
                        if day_kline_max_the_date == None or day_kline_max_the_date == '':
                            data_add_up = self.processing_single_security_code_all(security_code, exchange_code, data_add_up)
                        else:
                            # 根据day_kline_max_the_date已经处理的均线最大交易日
                            data_add_up = self.processing_single_security_code_increment(security_code, exchange_code, day_kline_max_the_date, data_add_up)

                        # 批量(10)列表的处理进度打印
                        if data_add_up % 10 == 0:
                            if data_add_up % 100 == 0:
                                data_process_line += '#'
                            processing = Decimal(data_add_up) / Decimal(len(stock_tuple)) * 100
                            print(datetime.datetime.now(), self.serviceName, 'processing data inner', 'stock_tuple size:', len(stock_tuple), 'processing ',
                                  data_process_line,
                                  str(processing) + '%')
                            # time.sleep(1)
                    except Exception:
                        data_add_up += 1
                        traceback.print_exc()
                # 最后一批增量列表的处理进度打印
                if data_add_up % 10 != 0:
                    if data_add_up % 100 == 0:
                        data_process_line += '#'
                    processing = Decimal(data_add_up) / Decimal(len(stock_tuple)) * 100
                    print(datetime.datetime.now(), self.serviceName, 'processing data outer', 'stock_tuple size:', len(stock_tuple), 'processing ',
                          data_process_line,
                          str(processing) + '%')
                    print(datetime.datetime.now(), self.serviceName, 'processing data all done ########################################')
                    # time.sleep(1)
        except Exception:
            traceback.print_exc()
        print(datetime.datetime.now(), self.serviceName, 'processing thread end ...', datetime.datetime.now())

    def processing_single_security_code_increment(self, security_code, exchange_code, day_kline_max_the_date, data_add_up):
        """
        处理增量单只股票的日K涨跌幅数据
        :param security_code: 股票代码
        :param exchange_code: 交易所
        :param day_kline_max_the_date: 已结处理过的最大交易日
        :param data_add_up: 处理进度增量标示
        :return:
        """
        print(datetime.datetime.now(), self.serviceName,
              'processing_single_security_code_increment 【start】 security_code:',
              security_code, 'exchange_code:', exchange_code,
              'data_add_up:', data_add_up)
        day_kline_tuple = self.dbService.query('select the_date, close from tquant_stock_day_kline '
                                               'where security_code = {security_code} '
                                               'and exchange_code = {exchange_code} '
                                               'and the_date >= {max_the_date} '
                                               'order by the_date asc '.format(security_code="'" + security_code + "'",
                                                                               exchange_code="'" + exchange_code + "'",
                                                                               max_the_date="'" + day_kline_max_the_date.strftime('%Y-%m-%d') + "'"
                                                                               ))
        data_add_up = self.process_day_kline_tuple(day_kline_tuple, security_code, exchange_code, data_add_up)
        print(datetime.datetime.now(), self.serviceName,
              'processing_single_security_code_increment 【end】 security_code:',
              security_code, 'exchange_code:', exchange_code,
              'data_add_up:', data_add_up)
        return data_add_up

    def processing_single_security_code_all(self, security_code, exchange_code, data_add_up):
        """
        处理单只股票的全部日K涨跌幅数据
        :param security_code: 股票代码
        :param exchange_code: 交易所
        :param data_add_up: 处理进度增量标示
        :return:
        """
        print(datetime.datetime.now(), self.serviceName,
              'processing_single_security_code_all 【start】 security_code:',
              security_code, 'exchange_code:', exchange_code,
              'data_add_up:', data_add_up)
        day_kline_tuple = self.dbService.query('select the_date, close from tquant_stock_day_kline '
                                               'where security_code = {security_code} '
                                               'and exchange_code = {exchange_code} '
                                               'order by the_date asc '.format(security_code="'" + security_code + "'",
                                                                               exchange_code="'" + exchange_code + "'"))
        data_add_up = self.process_day_kline_tuple(day_kline_tuple, security_code, exchange_code, data_add_up)
        print(datetime.datetime.now(), self.serviceName,
              'processing_single_security_code_all 【end】 security_code:',
              security_code, 'exchange_code:', exchange_code,
              'data_add_up:', data_add_up)
        return data_add_up

    def get_day_kline_max_the_date(self, security_code, exchange_code):
        """
        查询股票代码和交易所对应的已结处理过的日K站跌幅最大的交易日
        :param security_code: 股票代码
        :param exchange_code: 交易所
        :return:
        """
        sql = "select max(the_date) max_the_date from tquant_stock_day_kline " \
              "where security_code = {security_code} " \
              "and exchange_code = {exchange_code} " \
              "and previous_close is not null and fluctuate_percent is not null "
        the_date = self.dbService.query(sql.format(security_code="'"+security_code+"'",
                                                   exchange_code="'"+exchange_code+"'"
                                                   ))
        # print('get_day_kline_max_the_date:', the_date)
        if the_date:
            max_the_date = the_date[0][0]
            return max_the_date
        return None

    def process_day_kline_tuple(self, day_kline_tuple, security_code, exchange_code, data_add_up):
        """
        处理单只股票涨跌幅数据
        :param day_kline_tuple: 股票日K元组
        :param security_code: 股票代码
        :param exchange_code: 交易所
        :param data_add_up: 处理进度增量标识
        :return:
        """
        # 临时存储批量更新sql的列表
        upsert_sql_list = []
        # 需要处理的单只股票进度计数
        add_up = 0
        # 需要处理的单只股票进度打印字符
        process_line = ''
        # 循环处理security_code的股票日K数据
        i = 0
        while i < len(day_kline_tuple):
            # 切片元组，每相连的2个一组
            section_idx = i + 2
            if section_idx > len(day_kline_tuple):
                add_up += 1
                break
            temp_kline_tuple = day_kline_tuple[i:section_idx]
            # print(temp_kline_tuple)
            upsert_sql = self.analysis(temp_kline_tuple, security_code, exchange_code)
            # 批量(100)提交数据更新
            if len(upsert_sql_list) == 3000:
                self.dbService.insert_many(upsert_sql_list)
                process_line += '='
                upsert_sql_list = []
                upsert_sql_list.append(upsert_sql)
                processing = Decimal(add_up) / Decimal(len(day_kline_tuple)) * 100
                print(datetime.datetime.now(), self.serviceName, 'processing data inner', security_code, 'day_kline_tuple size:',
                      len(day_kline_tuple), 'processing ',
                      process_line,
                      str(processing) + '%')
                add_up += 1
                # 批量提交数据后当前线程休眠1秒
                # time.sleep(1)
            else:
                upsert_sql_list.append(upsert_sql)
                add_up += 1
            i += 1
        # 处理最后一批security_code的更新语句
        if len(upsert_sql_list) > 0:
            self.dbService.insert_many(upsert_sql_list)
            process_line += '='
            processing = Decimal(add_up) / Decimal(len(day_kline_tuple)) * 100
            print(datetime.datetime.now(), self.serviceName, 'processing data outer', security_code, 'day_kline_tuple size:',
                  len(day_kline_tuple), 'processing ', process_line,
                  str(processing) + '%')
        print(datetime.datetime.now(), self.serviceName, 'processing data ', security_code,
              ' done =============================================')
        # time.sleep(1)

        data_add_up += 1
        return data_add_up

    def analysis(self, temp_kline_tuple, security_code, exchange_code):
        """
        计算相邻两个K线后一个的涨跌幅和前一日收盘价
        :param temp_kline_tuple:
        :param security_code:
        :param exchange_code:
        :return:
        """
        # 需要处理的涨跌幅的交易日，即第二个元组的the_date
        the_date = temp_kline_tuple[1][0]
        # 前一日收盘价
        close1 = temp_kline_tuple[0][1]
        # 当前收盘价
        close2 = temp_kline_tuple[1][1]
        # 涨跌幅(百分比)计算:当日(收盘价-前一日收盘价)/前一日收盘价 * 100
        fluctuate_percent = ((close2 - close1) / close1) * 100
        upsert_sql = self.upsert.format(
            security_code="'" + security_code + "'",
            the_date="'" + the_date.strftime('%Y-%m-%d') + "'",
            exchange_code="'" + exchange_code + "'",
            previous_close=close1,
            fluctuate_percent=fluctuate_percent
        )
        return upsert_sql