# coding: utf-8
from decimal import *
import decimal
context = decimal.getcontext()
context.rounding = decimal.ROUND_05UP

import time
import sys

from com.listen.tquant.service.BaseService import BaseService


class StockDeriveKlineService(BaseService):
    """
    股票衍生K线数据处理服务
    """
    def __init__(self, dbService, kline_type, logger, sleep_seconds, one_time):
        super(StockDeriveKlineService, self).__init__(logger)
        self.base_info('{0[0]} ...', [self.get_current_method_name()])
        self.dbService = dbService
        self.kline_type = kline_type.strip()
        self.sleep_seconds = sleep_seconds
        self.one_time = one_time
        self.query_stock_sql = "select security_code, exchange_code " \
                               "from tquant_stock_day_kline " \
                               "group by security_code, exchange_code"

        self.upsert = "insert into tquant_stock_derive_kline " \
                      "(security_code, the_date, exchange_code, kline_type, " \
                      "open, high, low, " \
                      "close, previous_close, close_change_percent, " \
                      "amount, previous_amount, amount_change_percent, " \
                      "vol, previous_vol, vol_change_percent) " \
                      "values ({security_code}, {the_date}, {exchange_code}, {kline_type}, " \
                      "{open}, {high}, {low}, " \
                      "{close}, {previous_close}, {close_change_percent}, " \
                      "{amount}, {previous_amount}, {amount_change_percent}, " \
                      "{vol}, {previous_vol}, {vol_change_percent}) " \
                      "on duplicate key update " \
                      "open=values(open), high=values(high), low=values(low), " \
                      "close=values(close), previous_close=values(previous_close), amount_change_percent=values(amount_change_percent), " \
                      "amount=values(amount), previous_amount=values(previous_amount), amount_change_percent=values(amount_change_percent), " \
                      "vol=values(vol), previous_vol=values(previous_vol), vol_change_percent=values(vol_change_percent) "

        self.upsert_none = "insert into tquant_stock_derive_kline " \
                      "(security_code, the_date, exchange_code, kline_type, " \
                      "open, high, low, " \
                      "close, " \
                      "amount, " \
                      "vol)" \
                      "values ({security_code}, {the_date}, {exchange_code}, {kline_type}, " \
                      "{open}, {high}, {low}, " \
                      "{close}, " \
                      "{amount}, " \
                      "{vol}) " \
                      "on duplicate key update " \
                      "open=values(open), high=values(high), low=values(low), " \
                      "close=values(close), " \
                      "amount=values(amount), " \
                      "vol=values(vol) "

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
        self.base_info('{0[0]} {0[1]}【start】...', [self.get_current_method_name(), self.kline_type])
        try:
            # 需要处理的股票代码
            result = self.dbService.query(self.query_stock_sql)
            self.processing_security_codes(result, 'batch-0')
        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.base_error('{0[0]} {0[1]} {0[2]} {0[3]} {0[4]}',
                            [self.get_current_method_name(), self.kline_type, exc_type, exc_value, exc_traceback])
        self.base_info('{0[0]} {0[1]}【end】', [self.get_current_method_name(), self.kline_type])

    def processing_security_codes(self, tuple_security_codes, batch_name):
        self.base_info('{0[0]} {0[1]} {0[2]}【start】...', [self.get_current_method_name(), self.kline_type, batch_name])
        try:
            if tuple_security_codes:
                calendar_group = self.get_calendar_group_by_kline_type()
                len_result = len(tuple_security_codes)
                # 需要处理的股票代码进度计数
                data_add_up = 0
                # 需要处理的股票代码进度打印字符
                process_line = ''
                security_code = None
                exchange_code = None
                for stock_item in tuple_security_codes:
                    data_add_up += 1
                    try:
                        # 股票代码
                        security_code = stock_item[0]
                        exchange_code = stock_item[1]
                        # 根据security_code和exchange_code日K已经处理的最大交易日
                        kline_max_the_date = self.get_kline_max_the_date(security_code, exchange_code)
                        self.processing_single_security_code(security_code, exchange_code, calendar_group,
                                                             kline_max_the_date)
                        # 批量(10)列表的处理进度打印
                        if data_add_up % 10 == 0:
                            if data_add_up % 100 == 0:
                                process_line += '#'
                            processing = self.base_round(Decimal(data_add_up) / Decimal(len_result), 4) * 100
                            self.base_info('{0[0]} {0[1]} {0[2]} {0[3]} {0[4]} {0[5]} {0[6]}%...',
                                           [self.get_current_method_name(), self.kline_type, batch_name, 'inner', len_result, process_line,
                                            processing])
                    except Exception:
                        exc_type, exc_value, exc_traceback = sys.exc_info()
                        self.base_error('{0[0]} {0[1]} {0[2]} {0[3]} {0[4]}',
                                        [self.get_current_method_name(), self.kline_type,
                                         exc_type, exc_value, exc_traceback])
                # 最后一批增量列表的处理进度打印
                if data_add_up % 10 != 0:
                    process_line += '#'
                    processing = self.base_round(Decimal(data_add_up) / Decimal(len_result), 4) * 100
                    self.base_info('{0[0]} {0[1]} {0[2]} {0[3]} {0[4]} {0[5]} {0[6]}%',
                                   [self.get_current_method_name(), self.kline_type, batch_name,
                                    'outer', len_result, process_line,
                                    processing])
        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.base_error('{0[0]} {0[1]} {0[2]} {0[3]} {0[4]}',
                            [self.get_current_method_name(), self.kline_type, exc_type, exc_value, exc_traceback])
        self.base_info('{0[0]} {0[1]} {0[2]}【end】', [self.get_current_method_name(), self.kline_type, batch_name])

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
        self.base_debug('{0[0]} {0[1]} {0[2]} 【start】...',
                       [self.get_current_method_name(), security_code, exchange_code])
        # is_first就是是否第一次需要查询的标识
        is_first = True
        # 前一group收盘价，后面计算涨跌幅要用
        previous_close = None
        previous_amount = None
        previous_vol = None
        # 前一日均值
        previous_data = None
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
                elif kline_max_the_date >= group[0] and kline_max_the_date <= group[1] and is_first:
                    previous_data = self.get_kline_previous_data(group[0])
                    # 只在第一次做查询，无论查询是否有数据，后面都不再查询
                    is_first = False
                    if previous_data != None:
                        previous_close = previous_data[0][0]
                        previous_amount = previous_data[0][1]
                        previous_vol = previous_data[0][2]
                # else 分支说明往后的数据都需要处理，都可以认为是没有处理过的，所就没有else分支了，在此说明是注释一下
            # 所有的判断都做完了就可以真正的开始处理groupK数据了
            # 计算处理该group的K线数据
            # result格式为(the_date, open, high, low, close, close_change_percent, amount, amount_change_percent, vol, vol_change_percent)
            result = self.get_one_group_kline(group, security_code, exchange_code, previous_close, previous_amount, previous_vol)
            if result:
                if previous_close == None:
                    upsert_sql = self.upsert_none
                else:
                    upsert_sql = self.upsert
                upsert_sql = upsert_sql.format(security_code=self.quotes_surround(security_code),
                                               the_date=self.quotes_surround(result[0].strftime('%Y-%m-%d')),
                                               exchange_code=self.quotes_surround(exchange_code),
                                               kline_type=self.quotes_surround(self.kline_type),
                                               open=result[1],
                                               high=result[2],
                                               low=result[3],
                                               close=result[4],
                                               previous_close=previous_close,
                                               close_change_percent=result[5],
                                               amount=result[6],
                                               previous_amount=previous_amount,
                                               amount_change_percent=result[7],
                                               vol=result[8],
                                               previous_vol=previous_vol,
                                               vol_change_percent=result[9]
                                               )
                upsert_sql_list.append(upsert_sql)
                previous_close = result[4]
                previous_amount = result[6]
                previous_vol = result[8]
                if len(upsert_sql_list) % 100 == 0:
                    self.dbService.insert_many(upsert_sql_list)
                    upsert_sql_list = []
                    process_line += '='
                    processing = self.base_round(Decimal(add_up) / Decimal(len(calendar_group)), 4) * 100
                    self.base_debug('{0[0]} {0[1]} {0[2]} {0[3]} {0[4]} {0[5]} {0[6]} {0[7]}%...',
                                   [self.get_current_method_name(), self.kline_type, security_code, exchange_code, 'inner', len(calendar_group), process_line,
                                    processing])
        if len(upsert_sql_list) > 0:
            self.dbService.insert_many(upsert_sql_list)
            processing = self.base_round(Decimal(add_up) / Decimal(len(calendar_group)), 4) * 100
            self.base_debug('{0[0]} {0[1]} {0[2]} {0[3]} {0[4]} {0[5]} {0[6]} {0[7]}%',
                           [self.get_current_method_name(), self.kline_type, security_code, exchange_code, 'outer', len(calendar_group), process_line, processing])
        self.base_debug('{0[0]} {0[1]} {0[2]} 【end】',
                       [self.get_current_method_name(), security_code, exchange_code])

    def get_kline_previous_data(self, group_start_the_date):
        sql = "select close, amount, vol from tquant_stock_derive_kline " \
              "where kline_type = '" + self.kline_type + "' and the_date < '" + group_start_the_date.strftime('%Y-%m-%d') + "' order by the_date desc limit 1"
        previous_data = self.dbService.query(sql)
        return previous_data

    def get_one_group_kline(self, group, security_code, exchange_code, previous_close, previous_amount, previous_vol):
        """
        查询一个group时间内的日K数据，并处理成为一个self.kline_type K的数据
        :param group: 分组的时间，tupe(min_the_date, max_the_date)
        :param security_code: 股票代码
        :param exchange_code: 交易所
        :param previous_close:前一个K线的均价，有可能是None
        :param previous_amount:前一个K线的交易额，有可能是None
        :param previous_vol:前一个K线的交易量，有可能是None
        :return:
        """
        day_kline_tuple = self.dbService.query('select the_date, amount, vol, open, high, low, close '
                                               'from tquant_stock_day_kline '
                                               'where security_code = {security_code} '
                                               'and exchange_code = {exchange_code} '
                                               'and the_date >= {min_the_date} and the_date <= {max_the_date} '
                                               'order by the_date asc '.format(security_code=self.quotes_surround(security_code),
                                                                               exchange_code=self.quotes_surround(exchange_code),
                                                                               min_the_date=self.quotes_surround(group[0].strftime(
                                                                                   '%Y-%m-%d')),
                                                                               max_the_date=self.quotes_surround(group[1].strftime(
                                                                                   '%Y-%m-%d'))))
        if day_kline_tuple != None and len(day_kline_tuple) > 0:
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
            for day_kline in day_kline_tuple:
                amount += day_kline[1]
                vol += day_kline[2]
                if high < day_kline[4]:
                    high = day_kline[4]
                if low > day_kline[5]:
                    low = day_kline[5]
            # 周涨跌幅计算
            close_change_percent = None
            amount_change_percent = None
            vol_change_percent = None
            # 如果前一交易日均收盘价不为空，则计算当前交易日的均收盘价涨跌幅
            if previous_close != None:
                # 如果前一交易日的均收盘价不为0，则计算涨跌幅
                if previous_close != Decimal(0):
                    close_change_percent = self.base_round((close - previous_close) / previous_close, 4) * 100
                # 如果前一交易日的均收盘价为0，则设置涨跌幅为1，即100%
                else:
                    close_change_percent = self.base_round(Decimal(1), 4) * 100
            # 如果前一交易日均收盘价为空，则设置前一日均收盘价为当前均收盘价，涨跌幅为0，即0%
            else:
                previous_close = close
                close_change_percent = self.base_round(Decimal(0), 4) * 100

            # 如果前一交易日均交易额不为空，则计算当前交易日的均交易额涨跌幅
            if previous_amount != None:
                # 如果前一交易日的均交易额不为0，则计算涨跌幅
                if previous_amount != Decimal(0):
                    amount_change_percent = self.base_round((amount - previous_amount) / previous_amount, 4) * 100
                # 如果前一交易日的均交易额为0，则设置涨跌幅为1，即100%
                else:
                    amount_change_percent = self.base_round(Decimal(1), 4) * 100
            # 如果前一交易日均交易额为空，则设置前一日均交易量为当前均交易量，涨跌幅为0，即0%
            else:
                previous_amount = amount
                amount_change_percent = self.base_round(Decimal(0), 4) * 100

            # 如果前一交易日均交易量不为空，则计算当前交易日的均交易量涨跌幅
            if previous_vol != None:
                # 如果前一交易日的均交易额不为0，则计算涨跌幅
                if previous_vol != Decimal(0):
                    vol_change_percent = self.base_round((vol - previous_vol) / previous_vol, 4) * 100
                # 如果前一交易日的均交易量为0，则设置涨跌幅为1，即100%
                else:
                    vol_change_percent = self.base_round(Decimal(1), 4) * 100
            # 如果前一交易日均交易量为空，则设置前一日均交易量为当前均交易量，涨跌幅为0，即0%
            else:
                previous_vol = vol
                vol_change_percent = self.base_round(Decimal(0), 4) * 100
            return (the_date, open, high, low, close, close_change_percent, amount, amount_change_percent, vol, vol_change_percent)
        return None

    def get_kline_max_the_date(self, security_code, exchange_code):
        """
        查询股票代码和交易所对应的已结处理过的K线涨跌幅最大的交易日
        :param security_code: 股票代码
        :param exchange_code: 交易所
        :return:
        """
        sql = "select max(the_date) max_the_date " \
              "from tquant_stock_derive_kline " \
              "where security_code = {security_code} " \
              "and exchange_code = {exchange_code}"
        the_date = self.dbService.query(sql.format(security_code=self.quotes_surround(security_code),
                                                   exchange_code=self.quotes_surround(exchange_code)
                                                   ))
        if the_date:
            max_the_date = the_date[0][0]
            return max_the_date
        return None