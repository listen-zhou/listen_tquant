# coding: utf-8
import traceback
from decimal import *

import types

import numpy
import tquant as tt
import datetime
import time


class StockOtherKlineService():
    """
    股票日K数据涨跌幅处理服务
    """
    def __init__(self, dbService, kline_type):
        self.serviceName = 'StockOtherKlineService'
        self.dbService = dbService
        self.kline_type = kline_type
        print(datetime.datetime.now(), self.serviceName, self.kline_type, 'init ...', datetime.datetime.now())
        self.query_stock_sql = "select a.security_code, a.exchange_code " \
                               "from tquant_security_info a " \
                               "where a.security_type = 'STOCK'"
        if kline_type == 'week':
            self.kline_table = 'tquant_stock_week_kline'
        elif kline_type == 'month':
            self.kline_table = 'tquant_stock_month_kline'
        elif kline_type == 'quarter':
            self.kline_table = 'tquant_stock_quarter_kline'
        elif kline_type == 'year':
            self.kline_table = 'tquant_stock_year_kline'
        else:
            self.kline_table = None
        self.upsert = "insert into " + self.kline_table + " (security_code, the_date, exchange_code, " \
                         "amount, vol, open, high, low, close, previous_close, fluctuate_percent) " \
                         "values ({security_code}, {the_date}, {exchange_code}, {amount}, {vol}, {open}, {high}, {low}, {close}, {previous_close}, {fluctuate_percent}) " \
                         "on duplicate key update " \
                         "amount=values(amount), vol=values(vol), open=values(open), " \
                         "high=values(high), low=values(low), close=values(close), " \
                         "previous_close=values(previous_close), fluctuate_percent=values(fluctuate_percent) "

        self.upsert_none = "insert into " + self.kline_table + " (security_code, the_date, exchange_code, " \
                  "amount, vol, open, high, low, close) " \
                  "values ({security_code}, {the_date}, {exchange_code}, {amount}, {vol}, {open}, {high}, {low}, {close}) " \
                  "on duplicate key update " \
                  "amount=values(amount), vol=values(vol), open=values(open), " \
                  "high=values(high), low=values(low), close=values(close)"

    def processing(self):
        """
        查询股票周K数据，处理涨跌幅，并入库
        根据已有的股票代码，循环查询单个股票的日K数据
        :return:
        """
        print(datetime.datetime.now(), self.serviceName, self.kline_type, 'processing start ... {}'.format(datetime.datetime.now()))
        try:
            # 需要处理的股票代码
            stock_tuple = self.dbService.query(self.query_stock_sql)
            print(datetime.datetime.now(), self.serviceName, 'processing stock_tuple:', stock_tuple)
            if stock_tuple:
                calendar_group = self.get_calendar_group_by_kline_type()
                print('calendar_group:', calendar_group)
                stock_tuple_len = len(stock_tuple)
                # 需要处理的股票代码进度计数
                data_add_up = 0
                # 需要处理的股票代码进度打印字符
                data_process_line = ''
                for stock_item in stock_tuple:
                    try:
                        data_add_up += 1
                        # 股票代码
                        security_code = stock_item[0]
                        exchange_code = stock_item[1]
                        # 根据security_code和exchange_code日K已经处理的最大交易日
                        kline_max_the_date = self.get_kline_max_the_date(security_code, exchange_code)
                        # print('kline_max_the_date', kline_max_the_date)
                        self.processing_single_security_code(security_code, exchange_code, calendar_group, kline_max_the_date)
                        # 批量(10)列表的处理进度打印
                        if data_add_up % 10 == 0:
                            if data_add_up % 100 == 0:
                                data_process_line += '#'
                            processing = round(Decimal(data_add_up) / Decimal(len(stock_tuple)), 4) * 100
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
                    processing = round(Decimal(data_add_up) / Decimal(len(stock_tuple)), 4) * 100
                    print(datetime.datetime.now(), self.serviceName, 'processing data outer', 'stock_tuple size:', len(stock_tuple), 'processing ',
                          data_process_line,
                          str(processing) + '%')
                    print(datetime.datetime.now(), self.serviceName, 'processing data 【all done】 ########################################')
                    # time.sleep(1)
        except Exception:
            traceback.print_exc()
        print(datetime.datetime.now(), self.serviceName, 'processing thread end ...', datetime.datetime.now())

    def get_calendar_group_by_kline_type(self):
        if self.kline_type == 'week':
            sql = "select the_date, week_of_year from tquant_calendar_info order by the_date asc "
        elif self.kline_type == 'month':
            sql = "select the_date, month from tquant_calendar_info order by the_date asc "
        elif self.kline_type == 'quarter':
            sql = "select the_date, quarter from tquant_calendar_info order by the_date asc "
        elif self.kline_type == 'year':
            sql = "select the_date, year from tquant_calendar_info order by the_date asc "
        calendar_tuple = self.dbService.query(sql)
        if calendar_tuple:
            calendar_group_list = []
            # 交易日按从小到大正序排列，格式为the_date, kilne_type_number
            # 首先第一个时间分组为calendar_tuple[0][1]
            group = calendar_tuple[0][1]
            # 存储返回值，list里面是每一个group的开始日期和结束日期组合的tuple，格式为min_the_date, max_the_date
            the_date_list = []
            # 分组最小日期默认值为None
            min_the_date = None
            # 循环处理交易日数据的自增标识
            i = 0
            # 分组最大日期默认值为None
            max_the_date = None
            while i < len(calendar_tuple):
                # 如果分组group的数字和前一次相等，说明是同一分组
                if group == calendar_tuple[i][1]:
                    # 如果group内的最小交易日min_the_date == None，说明这是新的group，遇到的第一天就是最小日期
                    if min_the_date == None:
                        min_the_date = calendar_tuple[i][0]
                    # 如果i是最后一个下标，则append最后一个group到the_date_list中
                    if i == len(calendar_tuple) - 1:
                        max_the_date = calendar_tuple[i][0]
                        the_date_list.append((min_the_date, max_the_date))
                # 否则是新的group
                else:
                    # 既然是新group，则需要处理前一个group的数据append到the_date_list中，并充值参数，重新开始
                    the_date_list.append((min_the_date, max_the_date))
                    # 将group分组append到the_date_list中后，重置group的number为本次calendar_tuple[i][1]
                    group = calendar_tuple[i][1]
                    # 然后开始处理新的group交易日数据
                    # 因为是新的group，所以第一天是最小交易日
                    min_the_date = calendar_tuple[i][0]
                # 默认设置group的结束日期为每一天的交易日，防止group中只有一个元素时，最大交易日和最小交易日相等的情况
                max_the_date = calendar_tuple[i][0]
                # 继续下一次循环，i要增加1
                i += 1
            return the_date_list
        else:
            return None


    def processing_single_security_code(self, security_code, exchange_code, calendar_group, kline_max_the_date):
        """
        处理单只股票的全部日K涨跌幅数据
        :param security_code: 股票代码
        :param exchange_code: 交易所
        :param calendar_group: 交易日正序分组列表list_tuple,格式为[('2017-01-01', '2017-01-05'),...]
        :param kline_max_the_date: 已经处理过的K线的最大交易日，有可能为空，即一条也没有处理过
        :return:
        """
        print(datetime.datetime.now(), self.serviceName,
              'processing_single_security_code 【start】 security_code:',
              security_code, 'exchange_code:', exchange_code)
        # 前一group收盘价，后面计算涨跌幅要用
        previous_close = None
        upsert_sql_list = []
        add_up = 0
        process_line = ''
        for group in calendar_group:
            add_up += 1
            # 如果是增量处理模式，则需要找到增量的这个点的前一个K线的收盘价
            if kline_max_the_date != None and kline_max_the_date != '':
                # 如果不在group内，则说明还需要往后匹配
                if kline_max_the_date < group[0]:
                    continue
                # 如果在group内，则需要找到这group的前一group的收盘价
                elif kline_max_the_date >= group[0] and kline_max_the_date <= group[1]:
                    previous_close = self.get_kline_previous_close(group[0])
                # else 分支说明往后的数据都需要处理，都可以认为是没有处理过的，所就没有else分支了，在此说明是注释一下
            # 所有的判断都做完了就可以真正的开始处理groupK数据了
            # 计算处理该group的K线数据
            # 格式为(the_date, amount, vol, open, high, low, close, fluctuate_percent)
            one_group_kline = self.get_one_group_kline(group, previous_close, security_code, exchange_code)
            if one_group_kline:
                if previous_close == None:
                    upsert_sql = self.upsert_none.format(security_code="'" + security_code + "'",
                                                          the_date="'" + one_group_kline[0].strftime('%Y-%m-%d') + "'",
                                                          exchange_code="'" + exchange_code + "'",
                                                          amount=one_group_kline[1],
                                                          vol=one_group_kline[2],
                                                          open=one_group_kline[3],
                                                          high=one_group_kline[4],
                                                          low=one_group_kline[5],
                                                          close=one_group_kline[6])
                else:
                    upsert_sql = self.upsert.format(security_code="'" + security_code + "'",
                                                          the_date="'" + one_group_kline[0].strftime('%Y-%m-%d') + "'",
                                                          exchange_code="'" + exchange_code + "'",
                                                          amount=one_group_kline[1],
                                                          vol=one_group_kline[2],
                                                          open=one_group_kline[3],
                                                          high=one_group_kline[4],
                                                          low=one_group_kline[5],
                                                          close=one_group_kline[6],
                                                          previous_close=previous_close,
                                                          fluctuate_percent=one_group_kline[7])
                # print(upsert_sql)
                upsert_sql_list.append(upsert_sql)
                previous_close = one_group_kline[6]
                if len(upsert_sql_list) % 100 == 0:
                    self.dbService.insert_many(upsert_sql_list)
                    upsert_sql_list = []
                    process_line += '='
                    processing = round(Decimal(add_up) / Decimal(len(calendar_group)), 4) * 100
                    print(datetime.datetime.now(), self.serviceName, 'processing data inner', security_code, 'calendar_group size:',
                          len(calendar_group), 'processing ',
                          process_line,
                          str(processing) + '%')
        if len(upsert_sql_list) > 0:
            self.dbService.insert_many(upsert_sql_list)
            processing = round(Decimal(add_up) / Decimal(len(calendar_group)), 4) * 100
            print(datetime.datetime.now(), self.serviceName, 'processing data outer', security_code,
                  'calendar_group_by_week size:',
                  len(calendar_group), 'processing ',
                  process_line,
                  str(processing) + '%')
        print(datetime.datetime.now(), self.serviceName,
              'processing_single_security_code 【end】 security_code:',
              security_code, 'exchange_code:', exchange_code)

    def get_kline_previous_close(self, group_start_the_date):
        sql = "select close from " + self.kline_table + " where the_date < '" + group_start_the_date.strftime('%Y-%m-%d') + "' order by the_date desc limit 1"
        close = self.dbService.query(sql)
        if close:
            return close[0][0]
        return None

    def get_one_group_kline(self, group, previous_close, security_code, exchange_code):
        """
        查询一个group时间内的日K数据，并处理成为一个self.kline_type K的数据
        :param group: 分组的时间，tupe(min_the_date, max_the_date)
        :param previous_close:前一个K线的收盘价，有可能是None
        :param security_code: 股票代码
        :param exchange_code: 交易所
        :return:
        """
        day_kline_tuple = self.dbService.query('select the_date, amount, vol, open, high, low, close '
                                               'from tquant_stock_day_kline '
                                               'where security_code = {security_code} '
                                               'and exchange_code = {exchange_code} '
                                               'and the_date >= {min_the_date} and the_date <= {max_the_date} '
                                               'order by the_date asc '.format(security_code="'" + security_code + "'",
                                                                               exchange_code="'" + exchange_code + "'",
                                                                               min_the_date="'" + group[0].strftime(
                                                                                   '%Y-%m-%d') + "'",
                                                                               max_the_date="'" + group[1].strftime(
                                                                                   '%Y-%m-%d') + "'"))
        if day_kline_tuple:
            # 交易日，有可能交易日表group的group末比实际日K的交易日大，所以要以实际的为准
            the_date = day_kline_tuple[len(day_kline_tuple) - 1][0]
            # 周成交金额为周内成交金额之和
            amount = Decimal(0)
            # 周成交量为周内成交量之和
            vol = Decimal(0)
            # 周开盘价为一周第一天的开盘价
            open = day_kline_tuple[0][3]
            # 周最高价为一周内最高价
            high = day_kline_tuple[0][4]
            # 周最低价为一周内最低价
            low = day_kline_tuple[0][5]
            # 周收盘价为一周最后一天的收盘价
            close = day_kline_tuple[len(day_kline_tuple) - 1][6]
            # 周涨跌幅计算
            fluctuate_percent = None
            if previous_close != None and previous_close != Decimal(0):
                fluctuate_percent = (close - previous_close) / previous_close * 100
            for day_kline in day_kline_tuple:
                amount += day_kline[1]
                vol += day_kline[2]
                if high < day_kline[4]:
                    high = day_kline[4]
                if low > day_kline[5]:
                    low = day_kline[5]
            return (the_date, amount, vol, open, high, low, close, fluctuate_percent)
        return None

    def get_kline_max_the_date(self, security_code, exchange_code):
        """
        查询股票代码和交易所对应的已结处理过的K线涨跌幅最大的交易日
        :param security_code: 股票代码
        :param exchange_code: 交易所
        :return:
        """
        sql = "select max(the_date) max_the_date from " + self.kline_table + " " \
              "where security_code = {security_code} " \
              "and exchange_code = {exchange_code} " \
              "and previous_close is not null and fluctuate_percent is not null "
        the_date = self.dbService.query(sql.format(security_code="'"+security_code+"'",
                                                   exchange_code="'"+exchange_code+"'"
                                                   ))
        # print('get_kline_max_the_date:', the_date)
        if the_date:
            max_the_date = the_date[0][0]
            return max_the_date
        return None