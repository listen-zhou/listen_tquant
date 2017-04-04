# coding: utf-8

import decimal
from decimal import *
context = decimal.getcontext()
context.rounding = decimal.ROUND_05UP

import sys

from com.listen.tquant.service.BaseService import BaseService

import time


class StockAverageLineChgAvgService(BaseService):
    """
    股票均线涨跌幅数据处理服务
    """
    def __init__(self, dbService, ma, logger, sleep_seconds, one_time):
        super(StockAverageLineChgAvgService, self).__init__(logger)
        self.ma = ma
        self.dbService = dbService
        self.sleep_seconds = sleep_seconds
        self.one_time = one_time

        self.log_list = [self.get_clsss_name()]
        self.log_list.append('ma')
        self.log_list.append(ma)

        init_log_list = self.deepcopy_list(self.log_list)
        init_log_list.append(self.get_method_name())
        init_log_list.append('sleep seconds')
        init_log_list.append(sleep_seconds)
        init_log_list.append('one_time')
        init_log_list.append(one_time)
        self.logger.info(init_log_list)

        self.upsert = 'insert into tquant_stock_average_line (security_code, the_date, exchange_code, ' \
                      'ma, ' \
                      'close_avg_chg_avg, ' \
                      'amount_avg_chg_avg, ' \
                      'vol_avg_chg_avg, ' \
                      'price_avg_chg_avg, ' \
                      'amount_flow_chg_avg, vol_flow_chg_avg) ' \
                      'values ({security_code}, {the_date}, {exchange_code}, ' \
                      '{ma}, ' \
                      '{close_avg_chg_avg}, ' \
                      '{amount_avg_chg_avg}, ' \
                      '{vol_avg_chg_avg}, ' \
                      '{price_avg_chg_avg}, ' \
                      '{amount_flow_chg_avg}, {vol_flow_chg_avg}) ' \
                      'on duplicate key update ' \
                      'close_avg_chg_avg=values(close_avg_chg_avg), ' \
                      'amount_avg_chg_avg=values(amount_avg_chg_avg), ' \
                      'vol_avg_chg_avg=values(vol_avg_chg_avg), ' \
                      'price_avg_chg_avg=values(price_avg_chg_avg), ' \
                      'amount_flow_chg_avg=values(amount_flow_chg_avg), vol_flow_chg_avg=values(vol_flow_chg_avg) '

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
        # 需要处理的股票代码，查询股票基本信息表 security_code, exchange_code
        result = self.dbService.query_all_security_codes()
        self.processing_security_codes(processing_log_list, result, calendar_max_the_date, 'batch-0')

        end_log_list = self.deepcopy_list(processing_log_list)
        end_log_list.append('【end】')
        self.logger.info(end_log_list)

    def processing_security_codes(self, processing_log_list, tuple_security_codes, calendar_max_the_date, batch_name):
        if processing_log_list is not None and len(processing_log_list) > 0:
            security_codes_log_list = self.deepcopy_list(processing_log_list)
        else:
            security_codes_log_list = self.deepcopy_list(self.log_list)
        security_codes_log_list.append(self.get_method_name())
        security_codes_log_list.append(batch_name)

        len_result = len(tuple_security_codes)

        start_log_list = self.deepcopy_list(security_codes_log_list)
        start_log_list.append('tuple_security_codes size')
        start_log_list.append(len_result)
        start_log_list.append('calendar_max_the_date')
        start_log_list.append(calendar_max_the_date)
        start_log_list.append('【start】')
        self.logger.info(start_log_list)

        try:
            if tuple_security_codes is not None and len_result > 0:
                # 需要处理的股票代码进度计数
                add_up = 0
                # 需要处理的股票代码进度打印字符
                process_line = '#'
                for stock_item in tuple_security_codes:
                    add_up += 1
                    # 股票代码
                    security_code = stock_item[0]
                    exchange_code = stock_item[1]

                    # 根据security_code和exchange_code和ma查询ma均线已经处理的最大交易日
                    average_line_max_the_date = self.get_average_line_max_the_date(security_code, exchange_code)
                    # 如果均线已经处理的最大交易日和交易日表的最大交易日相等，说明无需处理该均线数据，继续下一个处理
                    if calendar_max_the_date == average_line_max_the_date:
                        warn_log_list = self.deepcopy_list(security_codes_log_list)
                        warn_log_list.append(security_code)
                        warn_log_list.append(exchange_code)
                        warn_log_list.append('average_line_max_the_date')
                        warn_log_list.append(average_line_max_the_date)
                        self.logger.warn(warn_log_list)
                        continue

                    # 根据average_line_max_the_date已经处理的均线最大交易日，获取递减ma个交易日后的交易日
                    decline_ma_the_date = self.get_calendar_decline_ma_the_date(average_line_max_the_date)
                    self.processing_single_security_code(security_codes_log_list, security_code, exchange_code, decline_ma_the_date)

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

    def get_average_line_max_the_date(self, security_code, exchange_code):
        sql = "select max(the_date) max_the_date from tquant_stock_average_line " \
              "where security_code = {security_code} " \
              "and exchange_code = {exchange_code} " \
              "and ma = {ma} " \
              "and close_avg_chg is not null " \
              "and amount_avg_chg is not null " \
              "and vol_avg_chg is not null " \
              "and price_avg_chg is not null " \
              "and amount_flow_chg is not null " \
              "and vol_flow_chg is not null "
        the_date = self.dbService.query(sql.format(security_code=self.quotes_surround(security_code),
                                                   exchange_code=self.quotes_surround(exchange_code),
                                                   ma=self.ma))
        if the_date:
            max_the_date = the_date[0][0]
            return max_the_date
        return None

    def get_calendar_decline_ma_the_date(self, average_line_max_the_date):
        if average_line_max_the_date is not None and average_line_max_the_date != '':
            sql = "select min(the_date) from (select the_date from tquant_calendar_info " \
                  "where the_date <= {average_line_max_the_date} " \
                  "order by the_date desc limit {ma}) a"
        else:
            sql = "select min(the_date) from tquant_calendar_info"
        the_date = self.dbService.query(sql.format(average_line_max_the_date=self.quotes_surround(str(average_line_max_the_date)),
                                                   ma=self.ma))
        if the_date is not None and the_date != '':
            decline_ma_the_date = the_date[len(the_date) - 1][0]
            return decline_ma_the_date
        return None

    def get_stock_day_kline(self, security_code, exchange_code, decline_ma_the_date):
        sql = "select the_date, " \
              "close_avg_chg, amount_avg_chg, vol_avg_chg, " \
              "price_avg_chg, amount_flow_chg, vol_flow_chg " \
              "from tquant_stock_average_line " \
              "where security_code = {security_code} " \
              "and exchange_code = {exchange_code} " \
              "and ma = {ma} " \
              "and close_avg_chg is not null " \
              "and amount_avg_chg is not null " \
              "and vol_avg_chg is not null " \
              "and price_avg_chg is not null " \
              "and amount_flow_chg is not null " \
              "and vol_flow_chg is not null "
        max_the_date = None
        if decline_ma_the_date is not None:
            sql += "and the_date >= {max_the_date} "
            max_the_date = decline_ma_the_date.strftime('%Y-%m-%d')
        sql += "order by the_date asc "
        sql = sql.format(security_code=self.quotes_surround(security_code),
                         exchange_code=self.quotes_surround(exchange_code),
                         max_the_date=self.quotes_surround(max_the_date),
                         ma=self.ma
                         )
        result = self.dbService.query(sql)
        return result

    def analysis(self, temp_line_tuple, security_code, exchange_code):

        # temp_line_tuple中的数据为the_date, close_avg_chg, amount_avg_chg,
        # vol_avg_chg, price_avg_chg, amount_flow_chg, vol_flow_chg
        # 当日the_date为正序排序最后一天的the_date，第一个元素
        the_date = temp_line_tuple[self.ma - 1][0]
        # 将元组元素转换为列表元素
        # temp_items = [item for item in temp_line_tuple[0:]]

        # ma日均收盘价涨跌幅均=sum(前ma日(含)均收盘价涨跌幅)/ma
        close_avg_chg_list = [close_avg_chg for close_avg_chg in [item[1] for item in temp_line_tuple]]
        close_avg_chg_avg = self.base_round(self.average(close_avg_chg_list), 2)

        # ma日均成交额涨跌幅均=sum(前ma日(含)均成交额涨跌幅)/ma
        amount_avg_chg_list = [amount_avg_chg for amount_avg_chg in [item[2] for item in temp_line_tuple]]
        amount_avg_chg_avg = self.base_round(self.average(amount_avg_chg_list), 2)

        # ma日均成交量涨跌幅均=sum(前ma日(含)均成交量涨跌幅)/ma
        vol_avg_chg_list = [vol_avg_chg for vol_avg_chg in [item[3] for item in temp_line_tuple]]
        vol_avg_chg_avg = self.base_round(self.average(vol_avg_chg_list), 2)

        # ma日均成交价涨跌幅均=sum(前ma日(含)均成交价涨跌幅)/ma
        price_avg_chg_list = [price_avg_chg for price_avg_chg in [item[4] for item in temp_line_tuple]]
        price_avg_chg_avg = self.base_round(self.average(price_avg_chg_list), 2)

        # 日金钱流向涨跌幅均=sum(前ma日(含)金钱流向涨跌幅)/ma
        amount_flow_chg_list = [amount_flow_chg for amount_flow_chg in [item[5] for item in temp_line_tuple]]
        amount_flow_chg_avg = self.base_round(self.average(amount_flow_chg_list), 2)

        # 日成交量流向涨跌幅均=sum(前ma日(含)成交量流向涨跌幅)/ma
        vol_flow_chg_list = [vol_flow_chg for vol_flow_chg in [item[5] for item in temp_line_tuple]]
        vol_flow_chg_avg = self.base_round(self.average(vol_flow_chg_list), 2)


        return [the_date,
                close_avg_chg_avg, amount_avg_chg_avg, vol_avg_chg_avg,
                price_avg_chg_avg, amount_flow_chg_avg, vol_flow_chg_avg]


    def processing_single_security_code(self, security_codes_log_list, security_code, exchange_code, decline_ma_the_date):
        """
        处理单只股票的均线数据
        :param security_code: 股票代码
        :param exchange_code: 交易所代码
        :param add_up: 针对批量处理股票代码时传入的进度参数
        :param decline_ma_the_date: 根据已经处理均线数据的最大交易日往前递减ma个交易日后的交易日，如果是单只股票执行，则可设置为1970-01-01日期
        :return: 返回批量处理时传入的进度累加值add_up
        """
        if security_codes_log_list is not None and len(security_codes_log_list) > 0:
            single_log_list = self.deepcopy_list(security_codes_log_list)
        else:
            single_log_list = self.deepcopy_list(self.log_list)
        single_log_list.append(self.get_method_name())
        single_log_list.append(security_code)
        single_log_list.append(exchange_code)
        single_log_list.append('decline_ma_the_date')
        single_log_list.append(decline_ma_the_date)

        start_log_list = self.deepcopy_list(single_log_list)
        start_log_list.append('【start】')
        self.logger.info(start_log_list)

        result = self.get_stock_day_kline(security_code, exchange_code, decline_ma_the_date)
        len_result = len(result)
        if len_result < self.ma:
            warn_log_list = self.deepcopy_list(single_log_list)
            warn_log_list.append('result len ')
            warn_log_list.append(len_result)
            warn_log_list.append('ma')
            warn_log_list.append(self.ma)
            self.logger.warn(warn_log_list)
            return

        try:
            if result is not None and len_result > 0:
                # 开始解析股票日K数据, the_date, close
                # 临时存储批量更新sql的列表
                upsert_sql_list = []
                # 需要处理的单只股票进度计数
                add_up = 0
                # 需要处理的单只股票进度打印字符
                process_line = '='
                # 循环处理security_code的股票日K数据
                i = 0
                while i < len_result:
                    add_up += 1
                    # 如果切片的下标是元祖的最后一个元素，则退出，因为已经处理完毕
                    if (i + self.ma) > len_result:
                        add_up -= 1
                        break
                    temp_line_tuple = result[i:(i + self.ma)]

                    # 返回值list_data list [the_date,
                    # close_avg_chg_avg, amount_avg_chg_avg, vol_avg_chg_avg,
                    # price_avg_chg_avg, amount_flow_chg_avg, vol_flow_chg_avg]
                    list_data = self.analysis(temp_line_tuple, security_code, exchange_code)
                    upsert_sql = self.upsert.format(security_code=self.quotes_surround(security_code),
                                                    the_date=self.quotes_surround(list_data[0].strftime('%Y-%m-%d')),
                                                    exchange_code=self.quotes_surround(exchange_code),
                                                    ma=self.ma,
                                                    close_avg_chg_avg=list_data[1],
                                                    amount_avg_chg_avg=list_data[2],
                                                    vol_avg_chg_avg=list_data[3],
                                                    price_avg_chg_avg=list_data[4],
                                                    amount_flow_chg_avg=list_data[5],
                                                    vol_flow_chg_avg=list_data[6]
                                                    )
                    # print(upsert_sql)

                    # 批量(100)提交数据更新
                    if len(upsert_sql_list) == 1000:
                        self.dbService.insert_many(upsert_sql_list)
                        process_line += '='
                        upsert_sql_list = []
                        upsert_sql_list.append(upsert_sql)
                        if len_result == self.ma:
                            processing = self.base_round(Decimal(1) * 100, 2)
                        else:
                            processing = self.base_round(Decimal(add_up) / Decimal(len_result - self.ma + 1) * 100, 2)

                        batch_log_list = self.deepcopy_list(single_log_list)
                        batch_log_list.append('inner')
                        batch_log_list.append(add_up + self.ma - 1)
                        batch_log_list.append(len_result)
                        batch_log_list.append(process_line)
                        batch_log_list.append(str(processing) + '%')
                        self.logger.info(batch_log_list)
                    else:
                        if upsert_sql is not None:
                            upsert_sql_list.append(upsert_sql)
                    i += 1

                # 处理最后一批security_code的更新语句
                if len(upsert_sql_list) > 0:
                    self.dbService.insert_many(upsert_sql_list)
                    process_line += '='
                if len_result == self.ma:
                    processing = self.base_round(Decimal(1) * 100, 2)
                else:
                    processing = self.base_round(Decimal(add_up) / Decimal(len_result - self.ma + 1) * 100, 2)

                batch_log_list = self.deepcopy_list(single_log_list)
                batch_log_list.append('outer')
                batch_log_list.append(add_up + self.ma - 1)
                batch_log_list.append(len_result)
                batch_log_list.append(process_line)
                batch_log_list.append(str(processing) + '%')
                self.logger.info(batch_log_list)
        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            exc_type, exc_value, exc_traceback = sys.exc_info()
            except_log_list = self.deepcopy_list(single_log_list)
            except_log_list.append(exc_type)
            except_log_list.append(exc_value)
            except_log_list.append(exc_traceback)
            self.logger.exception(except_log_list)

        end_log_list = self.deepcopy_list(single_log_list)
        end_log_list.append('【end】')
        self.logger.info(end_log_list)