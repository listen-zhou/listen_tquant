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
    def __init__(self, dbService, ma, logger, sleep_seconds, one_time):
        super(StockAverageLineService, self).__init__(logger)
        self.ma = ma
        self.dbService = dbService
        self.sleep_seconds = sleep_seconds
        self.one_time = one_time

        self.log_list = [self.get_classs_name()]
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
                      'close, close_avg, close_pre_avg, close_avg_chg, ' \
                      'amount, amount_avg, amount_pre_avg, amount_avg_chg, ' \
                      'vol, vol_avg, vol_pre_avg, vol_avg_chg, ' \
                      'price_avg, price_pre_avg, price_avg_chg, ' \
                      'amount_flow_chg, vol_flow_chg) ' \
                      'values ({security_code}, {the_date}, {exchange_code}, ' \
                      '{ma}, ' \
                      '{close}, {close_avg}, {close_pre_avg}, {close_avg_chg}, ' \
                      '{amount}, {amount_avg}, {amount_pre_avg}, {amount_avg_chg}, ' \
                      '{vol}, {vol_avg}, {vol_pre_avg}, {vol_avg_chg}, ' \
                      '{price_avg}, {price_pre_avg}, {price_avg_chg}, ' \
                      '{amount_flow_chg}, {vol_flow_chg}) ' \
                      'on duplicate key update ' \
                      'close=values(close), close_avg=values(close_avg), close_pre_avg=values(close_pre_avg), close_avg_chg=values(close_avg_chg), ' \
                      'amount=values(amount), amount_avg=values(amount_avg), amount_pre_avg=values(amount_pre_avg), amount_avg_chg=values(amount_avg_chg), ' \
                      'vol=values(vol), vol_avg=values(vol_avg), vol_pre_avg=values(vol_pre_avg), vol_avg_chg=values(vol_avg_chg), ' \
                      'price_avg=values(price_avg), price_pre_avg=values(price_pre_avg), price_avg_chg=values(price_avg_chg), ' \
                      'amount_flow_chg=values(amount_flow_chg), vol_flow_chg=values(vol_flow_chg) '

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
                security_code = None
                exchange_code = None
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
                        warn_log_list.append('calendar_max_the_date')
                        warn_log_list.append(calendar_max_the_date)
                        self.logger.warn(warn_log_list)
                        continue

                    # 根据average_line_max_the_date已经处理的均线最大交易日，获取递减ma个交易日后的交易日
                    decline_ma_the_date = self.get_calendar_decline_ma_the_date(average_line_max_the_date)
                    self.processing_single_security_code(security_codes_log_list, security_code, exchange_code, decline_ma_the_date)

                    # 批量(10)列表的处理进度打印
                    if add_up % 10 == 0:
                        process_line += '#'
                        processing = self.base_round_zero(add_up / (len_result) * 100, 2)
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
                processing = self.base_round_zero(add_up / (len_result) * 100, 2)
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

    def get_previous_data(self, security_code, exchange_code, the_date):
        sql = "select close_pre_avg, amount_pre_avg, vol_pre_avg, price_pre_avg from tquant_stock_average_line " \
              "where security_code = {security_code} " \
              "and exchange_code = {exchange_code} and ma = {ma} and the_date < {the_date} " \
              "order by the_date desc limit 1".format(security_code=self.quotes_surround(security_code),
                                                      exchange_code=self.quotes_surround(exchange_code),
                                                      ma=self.ma,
                                                      the_date=self.quotes_surround(the_date.strftime('%Y-%m-%d')))
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
            decline_ma_the_date = the_date[0][0]
            return decline_ma_the_date
        return None

    def get_stock_day_kline(self, security_code, exchange_code, decline_ma_the_date):
        sql = "select the_date, close, amount, vol " \
              "from tquant_stock_day_kline " \
              "where security_code = {security_code} " \
              "and exchange_code = {exchange_code} "
        max_the_date = None
        if decline_ma_the_date is not None:
            sql += "and the_date >= {max_the_date} "
            max_the_date = decline_ma_the_date.strftime('%Y-%m-%d')
        sql += "order by the_date asc "
        sql = sql.format(security_code=self.quotes_surround(security_code),
                         exchange_code=self.quotes_surround(exchange_code),
                         max_the_date=self.quotes_surround(max_the_date))
        result = self.dbService.query(sql)
        return result

    def analysis(self, temp_line_tuple, security_code, exchange_code, previous_data):

        # 前一ma日均收盘价，默认值为0，便于写入数据库
        close_pre_avg = self.base_round_zero(Decimal(0), 2)
        # 前一ma日均成交额(元)
        amount_pre_avg = self.base_round_zero(Decimal(0), 2)
        # 前一ma日均成交量(手)
        vol_pre_avg = self.base_round_zero(Decimal(0), 2)
        # 前一ma日均成交价
        price_pre_avg = self.base_round_zero(Decimal(0), 2)
        # close_pre_avg, amount_pre_avg, vol_pre_avg, price_pre_avg
        if previous_data is not None and len(previous_data) > 0:
            close_pre_avg = previous_data[0][0]
            amount_pre_avg = previous_data[0][1]
            vol_pre_avg = previous_data[0][2]
            price_pre_avg = previous_data[0][3]

        # temp_line_tuple中的数据为the_date, close, amount, vol
        # 当日the_date为正序排序最后一天的the_date，第一个元素
        the_date = temp_line_tuple[self.ma - 1][0]
        # 将元组元素转换为列表元素
        # temp_items = [item for item in temp_line_tuple[0:]]

        # 当日收盘价=正序排序最后一天的收盘价，最后一个元素的第2个元素
        close = temp_line_tuple[self.ma - 1][1]
        # ma日均收盘价=sum(前ma日(含)的收盘价)/ma
        close_list = [close for close in [item[1] for item in temp_line_tuple]]
        close_avg = self.base_round_zero(self.average_zero(close_list), 2)
        # 如果收盘ma日均价为None，则为异常数据，价格不可能为0
        # if close_avg is None:
        #     close_avg = self.base_round_zero(Decimal(0), 2)
        # ma日均收盘价涨跌幅=(ma日均收盘价 - 前一ma日均收盘价)/前一ma日均收盘价 * 100
        # 默认值为0
        close_avg_chg = self.base_round_zero(Decimal(0), 2)
        # if close_pre_avg is not None and close_pre_avg != Decimal(0):
        #     close_avg_chg = self.base_round_zero(self.division_zero((close_avg - close_pre_avg), close_pre_avg) * 100, 2)
        close_avg_chg = self.base_round_zero(self.division_zero((close_avg - close_pre_avg), close_pre_avg) * 100, 2)

        # 当日成交额
        amount = temp_line_tuple[self.ma - 1][2]
        # ma日均成交额=sum(前ma日(含)的成交额)/ma
        amount_list = [amount for amount in [item[2] for item in temp_line_tuple]]
        amount_avg = self.base_round_zero(self.average_zero(amount_list), 2)
        # if amount_avg is None :
        #     amount_avg = Decimal(0)
        # ma日均成交额涨跌幅=(ma日均成交额 - 前一ma日均成交额)/前一ma日均成交额 * 100
        # 默认值为0
        amount_avg_chg = self.base_round_zero(Decimal(0), 2)
        # if amount_pre_avg is not None and amount_pre_avg != Decimal(0):
        #     amount_avg_chg = self.base_round_zero(self.division_zero((amount_avg - amount_pre_avg), amount_pre_avg) * 100, 2)
        amount_avg_chg = self.base_round_zero(self.division_zero((amount_avg - amount_pre_avg), amount_pre_avg) * 100, 2)

        # 当日成交量
        vol = temp_line_tuple[self.ma - 1][3]
        # ma日均成交量=sum(前ma日(含)的成交量)/ma
        vol_list = [vol for vol in [item[3] for item in temp_line_tuple]]
        vol_avg = self.base_round_zero(self.average_zero(vol_list), 2)
        # if vol_avg is None:
        #     vol_avg = Decimal(0)
        # ma日均成交量涨跌幅=(ma日均成交量 - 前一ma日均成交量)/前一ma日均成交量 * 100
        vol_avg_chg = self.base_round_zero(Decimal(0), 2)
        # if vol_pre_avg is not None and vol_pre_avg != Decimal(0):
        #     vol_avg_chg = self.base_round_zero(self.division_zero((vol_avg - vol_pre_avg), vol_pre_avg) * 100, 2)
        vol_avg_chg = self.base_round_zero(self.division_zero((vol_avg - vol_pre_avg), vol_pre_avg) * 100, 2)

        # ma日均成交价=sum(前ma日(含)的成交额)/sum(ma日(含)的成交量)
        price_avg = self.base_round_zero(self.division_zero(self.sum_zero(amount_list), self.sum_zero(vol_list)), 2)
        # if price_avg is None:
        #     price_avg = Decimal(0)
        # ma日均成交价涨跌幅=(ma日均成交价 - 前一ma日均成交价)/前一ma日均成交价 * 100
        price_avg_chg = self.base_round_zero(Decimal(0), 2)
        # if price_pre_avg is not None and price_pre_avg != Decimal(0):
        #     price_avg_chg = self.base_round_zero(self.division_zero((price_avg - price_pre_avg), price_pre_avg) * 100, 2)
        price_avg_chg = self.base_round_zero(self.division_zero((price_avg - price_pre_avg), price_pre_avg) * 100, 2)

        # 日金钱流向涨跌幅=日成交额/ma日(含)均成交额 * 100
        amount_flow_chg = self.base_round_zero(self.division_zero(amount - amount_avg, amount_avg), 2)
        # if amount_flow_chg is None:
        #     amount_flow_chg = Decimal(0)

        # 日成交量流向涨跌幅=日成交量/ma日(含)均成交量 * 100
        vol_flow_chg = self.base_round_zero(self.division_zero(vol - vol_avg, vol_avg), 2)
        # if vol_flow_chg is None:
        #     vol_flow_chg = Decimal(0)

        return [the_date,
                close, close_avg, close_pre_avg, close_avg_chg,
                amount, amount_avg, amount_pre_avg, amount_avg_chg,
                vol, vol_avg, vol_pre_avg, vol_avg_chg,
                price_avg, price_pre_avg, price_avg_chg,
                amount_flow_chg, vol_flow_chg]


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

        result = self.get_stock_day_kline(security_code, exchange_code, decline_ma_the_date)
        len_result = len(result)
        start_log_list = self.deepcopy_list(single_log_list)
        start_log_list.append('result len ')
        start_log_list.append(len_result)
        start_log_list.append('【start】')
        self.logger.info(start_log_list)
        # 如果本次处理的数据条数小于ma，则无需处理
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
                # 由于是批量提交数据，所以在查询前一日均价时，有可能还未提交，
                # 所以只在第一次的时候查询，其他的情况用前一次计算的均价作为前一日均价
                # is_first就是是否第一次需要查询的标识
                # 前一日均值
                previous_data = None
                while i < len_result:
                    add_up += 1
                    # 如果切片的下标是元祖的最后一个元素，则退出，因为已经处理完毕
                    if (i + self.ma) > len_result:
                        add_up -= 1
                        break
                    temp_line_tuple = result[i:(i + self.ma)]
                    # 如果前一交易日的数据为空，则去查询一次
                    if previous_data is None or len(previous_data) == 0:
                        the_date = temp_line_tuple[self.ma - 1][0]
                        # close_pre_avg, amount_pre_avg, vol_pre_avg, price_pre_avg
                        previous_data = self.get_previous_data(security_code, exchange_code, the_date)
                        # 返回值list [the_date,
                        # close, close_avg, close_pre_avg, close_avg_chg,
                        # amount, amount_avg, amount_pre_avg, amount_avg_chg,
                        # vol, vol_avg, vol_pre_avg, vol_avg_chg,
                        # price_avg, price_pre_avg, price_avg_chg,
                        # amount_flow_chg, vol_flow_chg]
                    list_data = self.analysis(temp_line_tuple, security_code, exchange_code, previous_data)
                    upsert_sql = self.upsert.format(security_code=self.quotes_surround(security_code),
                                                    the_date=self.quotes_surround(list_data[0].strftime('%Y-%m-%d')),
                                                    exchange_code=self.quotes_surround(exchange_code),
                                                    ma=self.ma,
                                                    close=list_data[1],
                                                    close_avg=list_data[2],
                                                    close_pre_avg=list_data[3],
                                                    close_avg_chg=list_data[4],

                                                    amount=list_data[5],
                                                    amount_avg=list_data[6],
                                                    amount_pre_avg=list_data[7],
                                                    amount_avg_chg=list_data[8],

                                                    vol=list_data[9],
                                                    vol_avg=list_data[10],
                                                    vol_pre_avg=list_data[11],
                                                    vol_avg_chg=list_data[12],

                                                    price_avg=list_data[13],
                                                    price_pre_avg=list_data[14],
                                                    price_avg_chg=list_data[15],

                                                    amount_flow_chg=list_data[16],
                                                    vol_flow_chg=list_data[17]
                                                    )
                    # print(upsert_sql)
                    # 将本次的处理结果重新赋值到previous_data中
                    # close_pre_avg, amount_pre_avg, vol_pre_avg, price_pre_avg
                    previous_data = [[list_data[2], list_data[6], list_data[10], list_data[13]]]

                    # 批量(100)提交数据更新
                    if len(upsert_sql_list) == 1000:
                        self.dbService.insert_many(upsert_sql_list)
                        process_line += '='
                        upsert_sql_list = []
                        upsert_sql_list.append(upsert_sql)
                        if len_result == self.ma:
                            processing = self.base_round_zero(Decimal(1) * 100, 2)
                        else:
                            processing = self.base_round_zero(add_up / (len_result - self.ma + 1) * 100, 2)

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
                    processing = self.base_round_zero(Decimal(1) * 100, 2)
                else:
                    processing = self.base_round_zero(add_up / (len_result - self.ma + 1) * 100, 2)

                batch_log_list = self.deepcopy_list(single_log_list)
                batch_log_list.append('outer')
                batch_log_list.append(add_up + self.ma - 1)
                batch_log_list.append(len_result)
                batch_log_list.append(process_line)
                batch_log_list.append(str(processing) + '%')
                self.logger.info(batch_log_list)
        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            except_log_list = self.deepcopy_list(single_log_list)
            except_log_list.append(exc_type)
            except_log_list.append(exc_value)
            except_log_list.append(exc_traceback)
            self.logger.exception(except_log_list)

        end_log_list = self.deepcopy_list(single_log_list)
        end_log_list.append('result len ')
        end_log_list.append(len_result)
        end_log_list.append('【end】')
        self.logger.info(end_log_list)