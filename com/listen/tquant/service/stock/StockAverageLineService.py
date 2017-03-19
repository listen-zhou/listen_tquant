# coding: utf-8
import traceback
from decimal import *
import types

import numpy
import tquant as tt
import datetime
import time


class StockAverageLineService():
    """
    股票均线数据处理服务
    """
    def __init__(self, dbService, ma):
        self.serviceName = 'StockAverageLineService'
        self.ma = ma
        self.dbService = dbService
        self.business_type = 'STOCK_AVERAGE_' + str(ma) + 'DAY_LINE'
        print(datetime.datetime.now(), self.serviceName, 'ma', self.ma, 'init ...', datetime.datetime.now())
        # 股票基本信息表关联股票均线数据表，查询已经处理过均线的最新交易日
        self.query_stock_sql = "select a.security_code, a.exchange_code " \
                               "from tquant_security_info a " \
                               "where a.security_type = 'STOCK'"

        # 查询交易日表，根据上面sql查询的最大交易日，往前递减ma个交易日，并获取这个交易日
        self.query_decline_ma_sql = ""

        # 根据递减ma个交易日后的交易日，查询全部日K数据
        self.query_stock_day_kline_sql = "select the_date, close " \
                                         "from tquant_stock_day_kline " \
                                         "where security_code = {security_code} " \
                                         "and exchange_code = {exchange_code} " \
                                         "and the_date >= {max_the_date}" \
                                         "order by the_date asc "

        self.upsert = 'insert into tquant_stock_average_line (security_code, the_date, exchange_code, ' \
                 'ma, price) ' \
                 'values ({security_code}, {the_date}, {exchange_code}, ' \
                 '{ma}, {price}) ' \
                 'on duplicate key update ' \
                 'price=values(price)'

    def processing(self):
        """
        处理股票均线数据，并入库
        根据已有的股票代码，循环查询单个股票的日K数据
        :return:
        """
        print(datetime.datetime.now(), self.serviceName, 'ma', self.ma, 'processing start ... {}'.format(datetime.datetime.now()))
        getcontext().prec = 4
        try:
            # 获取交易日表最大交易日日期，类型为date.datetime
            calendar_max_the_date = self.get_calendar_max_the_date()
            # print('calendar_max_the_date:', calendar_max_the_date)
            # 需要处理的股票代码，查询股票基本信息表 security_code, exchange_code
            stock_tuple = self.dbService.query(self.query_stock_sql)
            print(datetime.datetime.now(), self.serviceName, 'ma', self.ma, 'processing stock_tuple:', stock_tuple)
            if stock_tuple:
                stock_tuple_len = len(stock_tuple)
                # 需要处理的股票代码进度计数
                data_add_up = 0
                # 需要处理的股票代码进度打印字符
                data_process_line = ''
                for stock_item in stock_tuple:
                    # 股票代码
                    security_code = stock_item[0]
                    exchange_code = stock_item[1]
                    # 根据security_code和exchange_code和ma查询ma均线已经处理的最大交易日
                    average_line_max_the_date = self.get_average_line_max_the_date(security_code, exchange_code)
                    # print('average_line_max_the_date:', average_line_max_the_date)
                    # 如果均线已经处理的最大交易日和交易日表的最大交易日相等，说明无需处理该均线数据，继续下一个处理
                    if calendar_max_the_date == average_line_max_the_date:
                        data_add_up += 1
                        print(datetime.datetime.now(), self.serviceName, 'ma', self.ma, 'security_code:',
                              security_code, 'exchange_code:', exchange_code,
                              'calendar_max_the_date:', calendar_max_the_date,
                              'average_line_max_the_date:', average_line_max_the_date, 'continue!!!')
                        continue
                    # 根据average_line_max_the_date已经处理的均线最大交易日，获取递减ma个交易日后的交易日
                    decline_ma_the_date = self.get_calendar_decline_ma_the_date(average_line_max_the_date)
                    data_add_up = self.processing_single_security_code(security_code, exchange_code, data_add_up, decline_ma_the_date)
                    # 批量(10)列表的处理进度打印
                    if data_add_up % 10 == 0:
                        if data_add_up % 100 == 0:
                            data_process_line += '#'
                        processing = Decimal(data_add_up) / Decimal(stock_tuple_len) * 100
                        print(datetime.datetime.now(), self.serviceName, 'ma', self.ma, 'processing data inner',
                              'stock_tuple size:', stock_tuple_len, 'processing ',
                              data_process_line,
                              str(processing) + '%')
                        # time.sleep(1)

                # 最后一批增量列表的处理进度打印
                if data_add_up % 10 != 0:
                    if data_add_up % 100 == 0:
                        data_process_line += '#'
                    processing = Decimal(data_add_up) / Decimal(len(stock_tuple)) * 100
                    print(datetime.datetime.now(), self.serviceName, 'ma', self.ma, 'processing data outer', 'stock_tuple size:', len(stock_tuple), 'processing ',
                          data_process_line,
                          str(processing) + '%')
                    print(datetime.datetime.now(), self.serviceName, 'ma', self.ma, 'processing data all done ########################################')
                    # time.sleep(1)
        except Exception:
            traceback.print_exc()
        print(datetime.datetime.now(), self.serviceName, 'ma', self.ma, 'processing thread end ...', datetime.datetime.now())


    def process_day_kline_tuple(self, day_kline_tuple, security_code, exchange_code, serviceName, data_add_up):
        # 开始解析股票日K数据, the_date, close
        # 临时存储批量更新sql的列表
        upsert_sql_list = []
        # 需要处理的单只股票进度计数
        add_up = 0
        # 需要处理的单只股票进度打印字符
        process_line = ''
        # 循环处理security_code的股票日K数据
        i = 0
        while i < len(day_kline_tuple):
            # 根据ma切片, 切片下标索引为i+self.ma
            section_idx = i + self.ma
            # 如果切片的下标是元祖的最后一个元素，则退出，因为已经处理完毕
            if section_idx > (len(day_kline_tuple)):
                break
            temp_line_tuple = day_kline_tuple[i:(i+self.ma)]
            # print('temp_line_tuple:', temp_line_tuple)
            # temp_line_tuple中的数据为the_date, close
            if temp_line_tuple and self.ma == len(temp_line_tuple):
                # 处理数据的交易日为切片的最后一个元素的the_date
                the_date = (temp_line_tuple[len(temp_line_tuple) - 1][0]).strftime('%Y-%m-%d')
                temp_items = [item for item in temp_line_tuple[0:]]
                price_list = [price for price in [item[1] for item in temp_items]]
                average_price = self.average_price(price_list)
                upsert_sql = self.upsert.format(
                    security_code="'"+security_code+"'",
                    the_date="'" + the_date + "'",
                    exchange_code="'" + exchange_code + "'",
                    ma=self.ma,
                    price=average_price
                )
                # 批量(100)提交数据更新
                if len(upsert_sql_list) == 3000:
                    self.dbService.insert_many(upsert_sql_list)
                    process_line += '='
                    upsert_sql_list = []
                    upsert_sql_list.append(upsert_sql)
                    if len(day_kline_tuple) == self.ma:
                        processing = 1.0
                    else:
                        processing = Decimal(add_up) / Decimal(len(day_kline_tuple) - self.ma) * 100
                    print(datetime.datetime.now(), serviceName, 'ma', self.ma, 'processing data inner', security_code, 'day_kline_tuple size:',
                          len(day_kline_tuple) - self.ma, 'processing ',
                          process_line,
                          str(processing) + '%')
                    # add_up += 1
                    # 批量提交数据后当前线程休眠1秒
                    # time.sleep(1)
                else:
                    upsert_sql_list.append(upsert_sql)
                    # add_up += 1
            i += 1
            add_up += 1
        # 处理最后一批security_code的更新语句
        if len(upsert_sql_list) > 0:
            self.dbService.insert_many(upsert_sql_list)
            process_line += '='
            # # 为了代码复用，增加一个是否增量标识，如果不是增量则更新进度记录，如果是增量则不做操作
            # if is_increment != True:
            #     # 更新进度记录
            #     data_dict = {}
            #     data_dict['business_type'] = self.business_type
            #     data_dict['security_code'] = security_code
            #     data_dict['security_type'] = 'STOCK'
            #     data_dict['exchange_code'] = exchange_code
            #     data_dict['process_progress'] = 1
            #     self.dbService.upsert_tquant_security_process_progress(data_dict)
            if len(day_kline_tuple) == self.ma:
                processing = 1.0
            else:
                processing = Decimal(add_up) / Decimal(len(day_kline_tuple) - self.ma) * 100
            print(datetime.datetime.now(), serviceName, 'ma', self.ma, 'processing data outer', security_code, 'day_kline_tuple size:',
                  len(day_kline_tuple) - self.ma, 'processing ', process_line,
                  str(processing) + '%')
        print(datetime.datetime.now(), serviceName, 'ma', self.ma, 'processing data ', security_code,
              ' done =============================================')
        # time.sleep(1)

        data_add_up += 1
        return data_add_up

    def average_price(self, price_list):
        if price_list and len(price_list) == self.ma:
            total_price = Decimal(0)
            for price in price_list:
                total_price += price
            average_price = total_price / Decimal(self.ma)
            return average_price
        return None

    def get_calendar_max_the_date(self):
        """
        查询交易日表中最大交易日日期
        :return:
        """
        sql = "select max(the_date) max_the_date from tquant_calendar_info"
        the_date = self.dbService.query(sql)
        # print('get_calendar_max_the_date:', the_date)
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
        # print('get_average_line_max_the_date:', the_date)
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
        # print('get_calendar_decline_ma_the_date:', the_date)
        if the_date != None and the_date != '':
            decline_ma_the_date = the_date[0][0]
            return decline_ma_the_date
        return None

    def processing_single_security_code(self, security_code, exchange_code, data_add_up, decline_ma_the_date):
        """
        处理单只股票的均线数据
        :param security_code: 股票代码
        :param exchange_code: 交易所代码
        :param data_add_up: 针对批量处理股票代码时传入的进度参数
        :param decline_ma_the_date: 根据已经处理均线数据的最大交易日往前递减ma个交易日后的交易日，如果是单只股票执行，则可设置为1970-01-01日期
        :return: 返回批量处理时传入的进度累加值data_add_up
        """
        try:
            getcontext().prec = 4
            # print('decline_ma_the_date:', decline_ma_the_date)
            print(datetime.datetime.now(), self.serviceName, 'ma', self.ma, 'processing_single_security_code start security_code:',
                  security_code, 'exchange_code:', exchange_code,
                  'decline_ma_the_date:', decline_ma_the_date)
            if decline_ma_the_date != None:
                day_kline_tuple = self.dbService.query(self.query_stock_day_kline_sql.format(
                    security_code="'" + security_code + "'",
                    exchange_code="'" + exchange_code + "'",
                    max_the_date="'" + decline_ma_the_date.strftime('%Y-%m-%d') + "'"
                ))
                print('day_kline_tuple:', day_kline_tuple)
                data_add_up = self.process_day_kline_tuple(day_kline_tuple, security_code, exchange_code,
                                                           self.serviceName, data_add_up)
            else:
                data_add_up += 1
        except Exception:
            data_add_up += 1
            traceback.print_exc()
        print(datetime.datetime.now(), self.serviceName, 'ma', self.ma,
              'processing_single_security_code end security_code:',
              security_code, 'exchange_code:', exchange_code,
              'decline_ma_the_date:', decline_ma_the_date)
        return data_add_up
