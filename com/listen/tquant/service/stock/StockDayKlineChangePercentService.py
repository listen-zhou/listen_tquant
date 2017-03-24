# coding: utf-8
from decimal import *
import decimal
context = decimal.getcontext()
context.rounding = decimal.ROUND_05UP

import traceback

import time
import sys

from com.listen.tquant.service.BaseService import BaseService


class StockDayKlineChangePercentService(BaseService):
    """
    股票日K数据涨跌幅处理服务
    """
    def __init__(self, dbService, logger):
        super(StockDayKlineChangePercentService, self).__init__(logger)
        self.dbService = dbService
        self.base_info('{0[0]} ...', [self.get_current_method_name()])
        self.query_stock_sql = "select a.security_code, a.exchange_code " \
                               "from tquant_security_info a " \
                               "where a.security_type = 'STOCK'"

        self.upsert = 'insert into tquant_stock_day_kline (security_code, the_date, exchange_code, ' \
                      'previous_close, close_change_percent, ' \
                      'previous_amount, amount_change_percent,' \
                      'previous_vol, vol_change_percent) ' \
                      'values ({security_code}, {the_date}, {exchange_code}, ' \
                      '{previous_close}, {close_change_percent},' \
                      '{previous_amount}, {amount_change_percent}, {previous_vol}, {vol_change_percent}) ' \
                      'on duplicate key update ' \
                      'previous_close=values(previous_close), close_change_percent=values(close_change_percent), ' \
                      'previous_amount=values(previous_amount), amount_change_percent=values(amount_change_percent), ' \
                      'previous_vol=values(previous_vol), vol_change_percent=values(vol_change_percent)'

    def processing(self):
        """
        根据已有的股票代码，循环查询单个股票的日K数据
        :return:
        """
        self.base_info('{0[0]} 【start】...', [self.get_current_method_name()])
        try:
            # 需要处理的股票代码
            result = self.dbService.query(self.query_stock_sql)
            if result != None and len(result) > 0:
                len_result = len(result)
                # 需要处理的股票代码进度计数
                data_add_up = 0
                # 需要处理的股票代码进度打印字符
                data_process_line = ''
                for stock_item in result:
                    data_add_up += 1
                    try:
                        # 股票代码
                        security_code = stock_item[0]
                        exchange_code = stock_item[1]
                        # 根据security_code和exchange_code日K已经处理的最大交易日
                        day_kline_max_the_date = self.get_day_kline_max_the_date(security_code, exchange_code)
                        # 根据day_kline_max_the_date已经处理的均线最大交易日，day_kline_max_the_date可为空，为空即为全部处理
                        self.processing_single_security_code(security_code, exchange_code, day_kline_max_the_date)
                        # 批量(10)列表的处理进度打印
                        if data_add_up % 10 == 0:
                            if data_add_up % 100 == 0:
                                data_process_line += '#'
                            processing = round(Decimal(data_add_up) / Decimal(len_result), 4) * 100
                            self.base_info('{0[0]} {0[1]} {0[2]} {0[3]} {0[4]}%...',
                                           [self.get_current_method_name(), 'inner', len_result, data_process_line,
                                            processing])
                            # time.sleep(1)
                    except Exception:
                        print(sys.exc_info())
                        exc_type, exc_value, exc_traceback = sys.exc_info()
                        self.base_error('{0[0]} {0[1]} {0[2]} {0[3]} ',
                                        [self.get_current_method_name(), exc_type, exc_value, exc_traceback])
                # 最后一批增量列表的处理进度打印
                if data_add_up % 10 != 0:
                    if data_add_up % 100 == 0:
                        data_process_line += '#'
                    processing = round(Decimal(data_add_up) / Decimal(len_result), 4) * 100
                    self.base_info('{0[0]} {0[1]} {0[2]} {0[3]} {0[4]}%',
                                   [self.get_current_method_name(), 'outer', len_result, data_process_line,
                                    processing])
        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.base_error('{0[0]} {0[1]} {0[2]} {0[3]} ',
                            [self.get_current_method_name(), exc_type, exc_value, exc_traceback])
        self.base_info('{0[0]} 【end】', [self.get_current_method_name()])

    def processing_single_security_code(self, security_code, exchange_code, day_kline_max_the_date):
        """
        处理增量单只股票的日K涨跌幅数据，如果day_kline_max_the_date为空，则全量处理
        :param security_code: 股票代码
        :param exchange_code: 交易所
        :param day_kline_max_the_date: 已结处理过的最大交易日
        :return:
        """
        self.base_info('{0[0]} {0[1]} {0[2]} 【start】...',
                       [self.get_current_method_name(), security_code, exchange_code])
        sql = "select the_date, close, amount, vol from tquant_stock_day_kline " \
              "where security_code = {security_code} " \
              "and exchange_code = {exchange_code} "
        max_the_date = None
        if day_kline_max_the_date != None and day_kline_max_the_date != '':
            sql += "and the_date >= {max_the_date} "
            max_the_date = day_kline_max_the_date.strftime('%Y-%m-%d')
        sql += "order by the_date asc "
        sql = sql.format(security_code="'" + security_code + "'",
                         exchange_code="'" + exchange_code + "'",
                         max_the_date="'" + str(max_the_date) + "'"
                         )
        print(sql)
        result = self.dbService.query(sql)
        # 临时存储批量更新sql的列表
        upsert_sql_list = []
        # 需要处理的单只股票进度计数
        add_up = 0
        # 需要处理的单只股票进度打印字符
        process_line = ''
        # 循环处理security_code的股票日K数据
        i = 0
        len_result = len(result)
        while i < len_result:
            add_up += 1
            # 切片元组，每相连的2个一组
            section_idx = i + 2
            if section_idx > len_result:
                break
            temp_kline_tuple = result[i:section_idx]
            upsert_sql = self.analysis(temp_kline_tuple, security_code, exchange_code)
            # 批量(100)提交数据更新
            if len(upsert_sql_list) == 3000:
                self.dbService.insert_many(upsert_sql_list)
                process_line += '='
                upsert_sql_list = []
                if upsert_sql != None:
                    upsert_sql_list.append(upsert_sql)
                processing = round(Decimal(add_up) / Decimal(len_result), 4) * 100
                self.base_debug('{0[0]} {0[1]} {0[2]} {0[3]} {0[4]}%...',
                                [self.get_current_method_name(), 'inner', len_result, process_line,
                                 processing])
                # 批量提交数据后当前线程休眠1秒
                # time.sleep(1)
            else:
                if upsert_sql != None:
                    upsert_sql_list.append(upsert_sql)
            i += 1
        # 处理最后一批security_code的更新语句
        if len(upsert_sql_list) > 0:
            self.dbService.insert_many(upsert_sql_list)
            process_line += '='
            processing = round(Decimal(add_up) / Decimal(len_result), 4) * 100
            self.base_debug('{0[0]} {0[1]} {0[2]} {0[3]} {0[4]}%',
                            [self.get_current_method_name(), 'outer', len_result, process_line,
                             processing])
        self.base_info('{0[0]} {0[1]} {0[2]} 【end】',
                       [self.get_current_method_name(), security_code, exchange_code])


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
              "and previous_close is not null and close_change_percent is not null "
        the_date = self.dbService.query(sql.format(security_code="'"+security_code+"'",
                                                   exchange_code="'"+exchange_code+"'"
                                                   ))
        if the_date:
            max_the_date = the_date[0][0]
            return max_the_date
        return None

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
        # 前一日交易额
        amount1 = temp_kline_tuple[0][2]
        # 当前交易额
        amount2 = temp_kline_tuple[1][2]
        # 前一日交易量
        vol1 = temp_kline_tuple[0][3]
        # 当前交易量
        vol2 = temp_kline_tuple[1][3]

        # 涨跌幅(百分比)计算:当日(收盘价-前一日收盘价)/前一日收盘价 * 100
        close_change_percent = None
        if close1 != None and close1 != Decimal(0):
            close_change_percent = round(((close2 - close1) / close1), 4) * 100
        amount_change_percent = None
        if amount1 != None and amount1 != Decimal(0):
            amount_change_percent = round(((amount2 - amount1) / amount1), 4) * 100
            vol_change_percent = None
        if vol1 != None and vol1 != Decimal(0):
            vol_change_percent = round(((vol2 - vol1) / vol1), 4) * 100
        if close1 != None and close_change_percent != None \
                and amount1 != None and amount_change_percent != \
                None and vol1 != None and vol_change_percent != None:
            upsert_sql = self.upsert.format(
                                            security_code="'" + security_code + "'",
                                            the_date="'" + the_date.strftime('%Y-%m-%d') + "'",
                                            exchange_code="'" + exchange_code + "'",
                                            previous_close=close1,
                                            close_change_percent=close_change_percent,
                                            previous_amount=amount1,
                                            amount_change_percent=amount_change_percent,
                                            previous_vol=vol1,
                                            vol_change_percent=vol_change_percent
                                        )
            return upsert_sql
        else:
            return None