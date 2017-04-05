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
    def __init__(self, dbService, logger, sleep_seconds, one_time):
        super(StockDayKlineChangePercentService, self).__init__(logger)
        self.dbService = dbService
        self.sleep_seconds = sleep_seconds
        self.one_time = one_time

        self.log_list = [self.get_classs_name()]

        init_log_list = self.deepcopy_list(self.log_list)
        init_log_list.append(self.get_method_name())
        init_log_list.append('sleep seconds')
        init_log_list.append(sleep_seconds)
        init_log_list.append('one_time')
        init_log_list.append(one_time)
        self.logger.info(init_log_list)

        self.upsert = 'insert into tquant_stock_day_kline (security_code, the_date, exchange_code, ' \
                      'previous_close, close_change_percent, ' \
                      'previous_amount, amount_change_percent, ' \
                      'previous_vol, vol_change_percent) ' \
                      'values ({security_code}, {the_date}, {exchange_code}, ' \
                      '{previous_close}, {close_change_percent},' \
                      '{previous_amount}, {amount_change_percent}, {previous_vol}, {vol_change_percent}) ' \
                      'on duplicate key update ' \
                      'previous_close=values(previous_close), close_change_percent=values(close_change_percent), ' \
                      'previous_amount=values(previous_amount), amount_change_percent=values(amount_change_percent), ' \
                      'previous_vol=values(previous_vol), vol_change_percent=values(vol_change_percent)'

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

        # 需要处理的股票代码
        result = self.dbService.query_all_security_codes()
        self.processing_security_codes(processing_log_list, result, 'batch-0')

        end_log_list = self.deepcopy_list(processing_log_list)
        end_log_list.append('【end】')
        self.logger.info(end_log_list)

    def processing_security_codes(self, processing_log_list, tuple_security_codes, batch_name):
        if processing_log_list is not None and len(processing_log_list) > 0:
            security_codes_log_list = self.deepcopy_list(processing_log_list)
        else:
            security_codes_log_list = self.deepcopy_list(self.log_list)
        security_codes_log_list.append(batch_name)

        len_result = len(tuple_security_codes)
        
        start_log_list = self.deepcopy_list(security_codes_log_list)
        start_log_list.append('tuple_security_codes size')
        start_log_list.append(len_result)
        start_log_list.append('【start】')
        self.logger.info(start_log_list)

        if len_result == 0:
            return

        try:
            # 需要处理的股票代码
            if tuple_security_codes is not None and len_result > 0:
                # 需要处理的股票代码进度计数
                add_up = 0
                # 需要处理的股票代码进度打印字符
                process_line = '#'
                security_code = None
                exchange_code = None
                for stock_item in tuple_security_codes:
                    add_up += 1
                    try:
                        # 股票代码
                        security_code = stock_item[0]
                        exchange_code = stock_item[1]

                        # 根据security_code和exchange_code日K已经处理的最大交易日
                        day_kline_max_the_date = self.get_day_kline_max_the_date(security_code, exchange_code)
                        # 根据day_kline_max_the_date已经处理的均线最大交易日，day_kline_max_the_date可为空，为空即为全部处理
                        self.processing_single_security_code(security_codes_log_list, security_code, exchange_code, day_kline_max_the_date)

                        # 批量(10)列表的处理进度打印
                        if add_up % 10 == 0:
                            process_line += '#'
                            processing = self.base_round(Decimal(add_up) / Decimal(len_result) * 100, 2)

                            batch_log_list = self.deepcopy_list(security_codes_log_list)
                            batch_log_list.append('inner')
                            batch_log_list.append(add_up)
                            batch_log_list.append(len_result)
                            batch_log_list.append(process_line)
                            batch_log_list.append(str(processing) + '%')
                            self.logger.info(batch_log_list)
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
                processing = self.base_round(Decimal(add_up) / Decimal(len_result) * 100, 2)

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
        end_log_list.append(len_result)
        end_log_list.append('【end】')
        self.logger.info(end_log_list)

    def get_stock_day_kline(self, security_code, exchange_code, start_date):
        """
        获取 >= start_date 交易日的股票日K数据，正序asc排列
        如果start_date is None，则查询全部日K
        :param security_code: 
        :param exchange_code: 
        :param start_date: 
        :return: 
        """
        sql = "select the_date, close, amount, vol from tquant_stock_day_kline " \
              "where security_code = {security_code} " \
              "and exchange_code = {exchange_code} "
        max_the_date = None
        if start_date is not None and start_date != '':
            sql += "and the_date >= {max_the_date} "
            max_the_date = start_date.strftime('%Y-%m-%d')
        sql += "order by the_date asc "
        sql = sql.format(security_code=self.quotes_surround(security_code),
                         exchange_code=self.quotes_surround(exchange_code),
                         max_the_date=self.quotes_surround(str(max_the_date))
                         )
        result = self.dbService.query(sql)
        return result

    def processing_single_security_code(self, security_codes_log_list, security_code, exchange_code, day_kline_max_the_date):
        """
        处理增量单只股票的日K涨跌幅数据，如果day_kline_max_the_date为空，则全量处理
        :param security_code: 股票代码
        :param exchange_code: 交易所
        :param day_kline_max_the_date: 已结处理过的最大交易日
        :return:
        """
        if security_codes_log_list is not None and len(security_codes_log_list) > 0:
            single_log_list = self.deepcopy_list(security_codes_log_list)
        else:
            single_log_list = self.deepcopy_list(self.log_list)
        single_log_list.append(self.get_method_name())
        single_log_list.append(security_code)
        single_log_list.append(exchange_code)
        single_log_list.append('day_kline_max_the_date')
        single_log_list.append(day_kline_max_the_date)

        result = self.get_stock_day_kline(security_code, exchange_code, day_kline_max_the_date)
        len_result = len(result)

        start_log_list = self.deepcopy_list(single_log_list)
        start_log_list.append('get_stock_day_kline result length')
        start_log_list.append(len_result)
        start_log_list.append('【start】')
        self.logger.info(start_log_list)

        if len_result == 0:
            return

        # 临时存储批量更新sql的列表
        upsert_sql_list = []
        # 需要处理的单只股票进度计数
        add_up = 0
        # 需要处理的单只股票进度打印字符
        process_line = ''
        # 循环处理security_code的股票日K数据
        i = 0
        while i < len_result:
            # 切片元组，每相连的2个一组
            section_idx = i + 2
            if section_idx > len_result:
                break
            temp_kline_tuple = result[i:section_idx]
            # 返回值格式list [the_date, close1, close_change_percent, amount1, amount_change_percent, vol1, vol_change_percent]
            list_data = self.analysis(single_log_list, temp_kline_tuple)
            if list_data is not None:
                upsert_sql = self.upsert.format(
                    security_code=self.quotes_surround(security_code),
                    the_date=self.quotes_surround(list_data[0].strftime('%Y-%m-%d')),
                    exchange_code=self.quotes_surround(exchange_code),
                    previous_close=list_data[1],
                    close_change_percent=list_data[2],
                    previous_amount=list_data[3],
                    amount_change_percent=list_data[4],
                    previous_vol=list_data[5],
                    vol_change_percent=list_data[6]
                )
            else:
                upsert_sql = None
                warn_log_list = self.deepcopy_list(single_log_list)
                warn_log_list.append('analysis list_data is None')
                self.logger.warn(warn_log_list)
            # 批量(100)提交数据更新
            if len(upsert_sql_list) == 1000:
                self.dbService.insert_many(upsert_sql_list)
                process_line += '='
                upsert_sql_list = []
                if upsert_sql is not None:
                    upsert_sql_list.append(upsert_sql)
                # 这个地方为什么要add_up + 1？因为第一条数据不会被处理，所以总数会少一条，所以在计算进度的时候要+1
                processing = self.base_round(Decimal(add_up + 1) / Decimal(len_result) * 100, 2)

                batch_log_list = self.deepcopy_list(single_log_list)
                batch_log_list.append('inner')
                batch_log_list.append(add_up + 1)
                batch_log_list.append(len_result)
                batch_log_list.append(process_line)
                batch_log_list.append(str(processing) + '%')
                self.logger.info(batch_log_list)
            else:
                if upsert_sql is not None:
                    upsert_sql_list.append(upsert_sql)
            i += 1
            add_up += 1
        # 处理最后一批security_code的更新语句
        if len(upsert_sql_list) > 0:
            self.dbService.insert_many(upsert_sql_list)
            process_line += '='
        processing = self.base_round(Decimal(add_up + 1) / Decimal(len_result) * 100, 2)
        batch_log_list = self.deepcopy_list(single_log_list)
        batch_log_list.append('outer')
        batch_log_list.append(add_up + 1)
        batch_log_list.append(len_result)
        batch_log_list.append(process_line)
        batch_log_list.append(str(processing) + '%')
        self.logger.info(batch_log_list)

        end_log_list = self.deepcopy_list(single_log_list)
        end_log_list.append('【end】')
        self.logger.info(end_log_list)


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
        the_date = self.dbService.query(sql.format(security_code=self.quotes_surround(security_code),
                                                   exchange_code=self.quotes_surround(exchange_code)
                                                   ))
        if the_date:
            max_the_date = the_date[0][0]
            return max_the_date
        return None

    def analysis(self, single_log_list, temp_kline_tuple):
        """
        计算相邻两个K线后一个的涨跌幅和前一日收盘价
        :param temp_kline_tuple:
        :param security_code:
        :param exchange_code:
        :return:
        """
        try:
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
            if close1 is not None and close1 != Decimal(0):
                close_change_percent = self.base_round(((close2 - close1) / close1) * 100, 2)
            else:
                close1 = self.base_round(Decimal(0), 2)
                close_change_percent = self.base_round(Decimal(0), 2)

            amount_change_percent = None
            if amount1 is not None and amount1 != Decimal(0):
                amount_change_percent = self.base_round(((amount2 - amount1) / amount1) * 100, 2)
            else:
                amount1 = self.base_round(Decimal(0), 2)
                amount_change_percent = self.base_round(Decimal(0), 2)

            vol_change_percent = None
            if vol1 is not None and vol1 != Decimal(0):
                vol_change_percent = self.base_round(((vol2 - vol1) / vol1) * 100, 2)
            else:
                vol1 = self.base_round(Decimal(0), 2)
                vol_change_percent = self.base_round(Decimal(0), 2)

            return [the_date, close1, close_change_percent, amount1, amount_change_percent, vol1, vol_change_percent]
        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            except_log_list = self.deepcopy_list(single_log_list)
            except_log_list.append(exc_type)
            except_log_list.append(exc_value)
            except_log_list.append(exc_traceback)
            self.logger.exception(except_log_list)
            return None
