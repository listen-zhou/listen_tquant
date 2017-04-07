# coding: utf-8

import tquant as tt
import sys
import numpy
from com.listen.tquant.utils.Utils import Utils
import logging
from com.listen.tquant.service.Service import Service
class StockOneStepBusinessService(Service):
    upsert_day_kline = 'insert into tquant_stock_day_kline (security_code, the_date, exchange_code, ' \
                  'amount, vol, open, high, low, close) ' \
                  'values ({security_code}, {the_date}, {exchange_code}, ' \
                  '{amount}, {vol}, {open}, {high}, {low}, {close}) ' \
                  'on duplicate key update ' \
                  'amount=values(amount), vol=values(vol), open=values(open), ' \
                  'high=values(high), low=values(low), close=values(close) '

    upsert_day_kline_change_percent = 'insert into tquant_stock_day_kline (security_code, the_date, exchange_code, ' \
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

    upsert_average_line = 'insert into tquant_stock_average_line (security_code, the_date, exchange_code, ' \
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

    upsert_average_line_avg = 'insert into tquant_stock_average_line (security_code, the_date, exchange_code, ' \
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

    def __init__(self, dbService, logger, mas, security_codes, is_reset):
        self.dbService = dbService
        self.logger = logger
        self.mas = mas
        self.security_codes = security_codes
        self.is_reset = is_reset
        self.log_list = [self.get_classs_name()]

        init_log_list = Utils.deepcopy_list(self.log_list)
        init_log_list.append(self.get_method_name())
        init_log_list.append('mas')
        init_log_list.append(mas)
        init_log_list.append('security_codes')
        init_log_list.append(security_codes)
        init_log_list.append('is_reset')
        init_log_list.append(is_reset)
        self.logger.base_log(init_log_list)

    def processing(self):
        start_log_list = Utils.deepcopy_list(self.log_list)
        start_log_list.append(self.get_method_name())
        start_log_list.append('【start】...')
        self.logger.base_log(start_log_list)

        if self.security_codes is not None and len(self.security_codes) > 0:
            for item in self.security_codes:
                if item is not None and len(item) == 2:
                    security_code = item[0]
                    exchange_code = item[1]
                    self.processing_single_security_code(security_code, exchange_code)

        end_log_list = Utils.deepcopy_list(self.log_list)
        end_log_list.append(self.get_method_name())
        end_log_list.append('【end】')
        self.logger.base_log(end_log_list)

    def processing_single_security_code(self, security_code, exchange_code):
        start_log_list = Utils.deepcopy_list(self.log_list)
        start_log_list.append(self.get_method_name())
        start_log_list.append('security_code')
        start_log_list.append(security_code)
        start_log_list.append('exchange_code')
        start_log_list.append(exchange_code)
        start_log_list.append('【start】...')
        self.logger.base_log(start_log_list)

        self.processing_day_kline(security_code, exchange_code)
        self.processing_day_kline_change_percent(security_code, exchange_code)
        if self.mas is not None and len(self.mas) > 0:
            for ma in self.mas:
                self.processing_average_line(ma, security_code, exchange_code)
                self.processing_average_line_avg(ma, security_code, exchange_code)

        end_log_list = Utils.deepcopy_list(self.log_list)
        end_log_list.append(self.get_method_name())
        end_log_list.append('security_code')
        end_log_list.append(security_code)
        end_log_list.append('exchange_code')
        end_log_list.append(exchange_code)
        end_log_list.append('【end】')
        self.logger.base_log(end_log_list)

    def processing_day_kline(self, security_code, exchange_code, recent_few_days=10):
        try:
            start_log_list = Utils.deepcopy_list(self.log_list)
            start_log_list.append(self.get_method_name())
            start_log_list.append('security_code')
            start_log_list.append(security_code)
            start_log_list.append('exchange_code')
            start_log_list.append(exchange_code)
            start_log_list.append('【start】...')
            self.logger.base_log(start_log_list)

            if self.is_reset:
                result = tt.get_all_daybar(security_code, 'qfq')
            else:
                result = tt.get_last_n_daybar(security_code, recent_few_days, 'qfq')

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
                        add_up += 1
                        # 解析股票日K数据（每行）
                        # 解析每行的返回值格式为list [the_date, amount, vol, open, high, low, close]
                        list_data = self.analysis_columns_day_kline(result, idx)
                        if list_data is not None:
                            upsert_sql = StockOneStepBusinessService.upsert_day_kline.format(
                                security_code=Utils.quotes_surround(security_code),
                                the_date=Utils.quotes_surround(list_data[0]),
                                exchange_code=Utils.quotes_surround(exchange_code),
                                amount=list_data[1],
                                vol=list_data[2],
                                open=list_data[3],
                                high=list_data[4],
                                low=list_data[5],
                                close=list_data[6]
                            )
                        else:
                            continue
                        # 批量(100)提交数据更新
                        if len(upsert_sql_list) == 3000:
                            self.dbService.insert_many(upsert_sql_list)
                            process_line += '='
                            upsert_sql_list = []
                            if upsert_sql is not None:
                                upsert_sql_list.append(upsert_sql)
                            progress = Utils.base_round(add_up / len_indexes * 100, 2)

                            progress_log_list = Utils.deepcopy_list(self.log_list)
                            progress_log_list.append(self.get_method_name())
                            progress_log_list.append('security_code')
                            progress_log_list.append(security_code)
                            progress_log_list.append('exchange_code')
                            progress_log_list.append(exchange_code)
                            progress_log_list.append('progress')
                            progress_log_list.append(add_up)
                            progress_log_list.append(len_indexes)
                            progress_log_list.append(process_line)
                            progress_log_list.append(progress)
                            self.logger.base_log(progress_log_list)
                        else:
                            upsert_sql_list.append(upsert_sql)
                    # 处理最后一批security_code的更新语句
                    if len(upsert_sql_list) > 0:
                        self.dbService.insert_many(upsert_sql_list)
                        process_line += '='
                    progress = Utils.base_round(add_up / len(indexes_values) * 100, 2)

                    progress_log_list = Utils.deepcopy_list(self.log_list)
                    progress_log_list.append(self.get_method_name())
                    progress_log_list.append('security_code')
                    progress_log_list.append(security_code)
                    progress_log_list.append('exchange_code')
                    progress_log_list.append(exchange_code)
                    progress_log_list.append('progress')
                    progress_log_list.append(add_up)
                    progress_log_list.append(len_indexes)
                    progress_log_list.append(process_line)
                    progress_log_list.append(progress)
                    self.logger.base_log(progress_log_list)
                else:
                    warn_log_list = Utils.deepcopy_list(self.log_list)
                    warn_log_list.append(self.get_method_name())
                    warn_log_list.append('security_code')
                    warn_log_list.append(security_code)
                    warn_log_list.append('exchange_code')
                    warn_log_list.append(exchange_code)
                    warn_log_list.append('result`s indexes_values is None')
                    self.logger.base_log(warn_log_list, logging.WARNING)
            else:
                warn_log_list = Utils.deepcopy_list(self.log_list)
                warn_log_list.append(self.get_method_name())
                warn_log_list.append('security_code')
                warn_log_list.append(security_code)
                warn_log_list.append('exchange_code')
                warn_log_list.append(exchange_code)
                warn_log_list.append('tt.get_all_daybar or tt.get_last_n_daybar result is None')
                self.logger.base_log(warn_log_list, logging.WARNING)

        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            error_log_list = Utils.deepcopy_list(self.log_list)
            error_log_list.append(self.get_method_name())
            error_log_list.append('security_code')
            error_log_list.append(security_code)
            error_log_list.append('exchange_code')
            error_log_list.append(exchange_code)
            error_log_list.append(exc_type)
            error_log_list.append(exc_value)
            error_log_list.append(exc_traceback)
            self.logger.base_log(error_log_list, logging.ERROR)

        end_log_list = Utils.deepcopy_list(self.log_list)
        end_log_list.append(self.get_method_name())
        end_log_list.append('security_code')
        end_log_list.append(security_code)
        end_log_list.append('exchange_code')
        end_log_list.append(exchange_code)
        end_log_list.append('【end】')
        self.logger.base_log(end_log_list)

    def analysis_columns_day_kline(self, day_kline, idx):
        try:
            the_date = (str(idx))[0:10]

            amount = day_kline.at[idx, 'amount']
            # amount的类型为numpy.ndarray，是一个多维数组，可能包含多个值，其他的字段也是一样，测试的时候发现有异常抛出
            if isinstance(amount, numpy.ndarray) and amount.size > 1:
                amount = amount.tolist()[0]
            amount = Utils.base_round(amount, 2)

            vol = day_kline.at[idx, 'vol']
            if isinstance(vol, numpy.ndarray) and vol.size > 1:
                vol = vol.tolist()[0]
            vol = Utils.base_round(vol, 2)

            open = day_kline.at[idx, 'open']
            if isinstance(open, numpy.ndarray) and open.size > 1:
                open = open.tolist()[0]
            open = Utils.base_round(open, 2)

            high = day_kline.at[idx, 'high']
            if isinstance(high, numpy.ndarray) and high.size > 1:
                high = high.tolist()[0]
            high = Utils.base_round(high, 2)

            low = day_kline.at[idx, 'low']
            if isinstance(low, numpy.ndarray) and low.size > 1:
                low = low.tolist()[0]
            low = Utils.base_round(low, 2)

            close = day_kline.at[idx, 'close']
            if isinstance(close, numpy.ndarray) and close.size > 1:
                close = close.tolist()[0]
            close = Utils.base_round(close, 2)

            return [the_date, amount, vol, open, high, low, close]
        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            error_log_list = Utils.deepcopy_list(self.log_list)
            error_log_list.append(self.get_method_name())
            error_log_list.append(exc_type)
            error_log_list.append(exc_value)
            error_log_list.append(exc_traceback)
            self.logger.base_log(error_log_list, logging.ERROR)
            return None

    def processing_day_kline_change_percent(self, security_code, exchange_code):
        start_log_list = Utils.deepcopy_list(self.log_list)
        start_log_list.append(self.get_method_name())
        start_log_list.append('security_code')
        start_log_list.append(security_code)
        start_log_list.append('exchange_code')
        start_log_list.append(exchange_code)
        start_log_list.append('【start】...')
        self.logger.base_log(start_log_list)

        if self.is_reset:
            day_kline_max_the_date = None
        else:
            day_kline_max_the_date = self.dbService.get_day_kline_max_the_date(security_code, exchange_code)
        result = self.dbService.get_stock_day_kline(security_code, exchange_code, day_kline_max_the_date)
        len_result = len(result)

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
            list_data = self.analysis_day_kline_change_percent(temp_kline_tuple)
            if list_data is not None:
                upsert_sql = StockOneStepBusinessService.upsert_day_kline_change_percent.format(
                    security_code=Utils.quotes_surround(security_code),
                    the_date=Utils.quotes_surround(list_data[0].strftime('%Y-%m-%d')),
                    exchange_code=Utils.quotes_surround(exchange_code),
                    previous_close=list_data[1],
                    close_change_percent=list_data[2],
                    previous_amount=list_data[3],
                    amount_change_percent=list_data[4],
                    previous_vol=list_data[5],
                    vol_change_percent=list_data[6]
                )
            else:
                upsert_sql = None
            # 批量(100)提交数据更新
            if len(upsert_sql_list) == 1000:
                self.dbService.insert_many(upsert_sql_list)
                process_line += '='
                upsert_sql_list = []
                if upsert_sql is not None:
                    upsert_sql_list.append(upsert_sql)
                # 这个地方为什么要add_up + 1？因为第一条数据不会被处理，所以总数会少一条，所以在计算进度的时候要+1
                progress = Utils.base_round((add_up + 1) / len_result * 100, 2)

                progress_log_list = Utils.deepcopy_list(self.log_list)
                progress_log_list.append(self.get_method_name())
                progress_log_list.append('security_code')
                progress_log_list.append(security_code)
                progress_log_list.append('exchange_code')
                progress_log_list.append(exchange_code)
                progress_log_list.append('progress')
                progress_log_list.append(add_up)
                progress_log_list.append(len_result)
                progress_log_list.append(process_line)
                progress_log_list.append(progress)
                self.logger.base_log(progress_log_list)
            else:
                if upsert_sql is not None:
                    upsert_sql_list.append(upsert_sql)
            i += 1
            add_up += 1
        # 处理最后一批security_code的更新语句
        if len(upsert_sql_list) > 0:
            self.dbService.insert_many(upsert_sql_list)
            process_line += '='
        progress = Utils.base_round((add_up + 1) / len_result * 100, 2)

        progress_log_list = Utils.deepcopy_list(self.log_list)
        progress_log_list.append(self.get_method_name())
        progress_log_list.append('security_code')
        progress_log_list.append(security_code)
        progress_log_list.append('exchange_code')
        progress_log_list.append(exchange_code)
        progress_log_list.append('progress')
        progress_log_list.append(add_up)
        progress_log_list.append(len_result)
        progress_log_list.append(process_line)
        progress_log_list.append(progress)
        self.logger.base_log(progress_log_list)

        end_log_list = Utils.deepcopy_list(self.log_list)
        end_log_list.append(self.get_method_name())
        end_log_list.append('security_code')
        end_log_list.append(security_code)
        end_log_list.append('exchange_code')
        end_log_list.append(exchange_code)
        end_log_list.append('【end】')
        self.logger.base_log(end_log_list)


    def analysis_day_kline_change_percent(self, temp_kline_tuple):
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
            if close1 is not None and close1 != 0:
                close_change_percent = Utils.base_round(((close2 - close1) / close1) * 100, 2)
            else:
                close1 = 0
                close_change_percent = 0

            amount_change_percent = None
            if amount1 is not None and amount1 != 0:
                amount_change_percent = Utils.base_round(((amount2 - amount1) / amount1) * 100, 2)
            else:
                amount1 = 0
                amount_change_percent = 0

            vol_change_percent = None
            if vol1 is not None and vol1 != 0:
                vol_change_percent = Utils.base_round(((vol2 - vol1) / vol1) * 100, 2)
            else:
                vol1 = 0
                vol_change_percent = 0

            return [the_date, close1, close_change_percent, amount1, amount_change_percent, vol1, vol_change_percent]
        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            error_log_list = Utils.deepcopy_list(self.log_list)
            error_log_list.append(self.get_method_name())
            error_log_list.append(exc_type)
            error_log_list.append(exc_value)
            error_log_list.append(exc_traceback)
            self.logger.base_log(error_log_list, logging.ERROR)
            return None

    def processing_average_line(self, ma, security_code, exchange_code):
        start_log_list = Utils.deepcopy_list(self.log_list)
        start_log_list.append(self.get_method_name())
        start_log_list.append(ma)
        start_log_list.append('ma')
        start_log_list.append('security_code')
        start_log_list.append(security_code)
        start_log_list.append('exchange_code')
        start_log_list.append(exchange_code)
        start_log_list.append('【start】...')
        self.logger.base_log(start_log_list)

        if self.is_reset:
            decline_ma_the_date = None
        else:
            average_line_max_the_date = self.dbService.get_average_line_max_the_date(ma, security_code, exchange_code)
            decline_ma_the_date = self.dbService.get_average_line_decline_max_the_date(ma, average_line_max_the_date)
        result = self.dbService.get_stock_day_kline(security_code, exchange_code, decline_ma_the_date)
        len_result = len(result)
        if len_result < ma:
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
                    if (i + ma) > len_result:
                        add_up -= 1
                        break
                    temp_line_tuple = result[i:(i + ma)]
                    # 如果前一交易日的数据为空，则去查询一次
                    if previous_data is None or len(previous_data) == 0:
                        the_date = temp_line_tuple[ma - 1][0]
                        # close_pre_avg, amount_pre_avg, vol_pre_avg, price_pre_avg
                        previous_data = self.dbService.get_previous_average_line(ma, security_code, exchange_code, the_date)
                        # 返回值list [the_date,
                        # close, close_avg, close_pre_avg, close_avg_chg,
                        # amount, amount_avg, amount_pre_avg, amount_avg_chg,
                        # vol, vol_avg, vol_pre_avg, vol_avg_chg,
                        # price_avg, price_pre_avg, price_avg_chg,
                        # amount_flow_chg, vol_flow_chg]
                    list_data = self.analysis_average_line(ma, temp_line_tuple, security_code, exchange_code, previous_data)
                    upsert_sql = StockOneStepBusinessService.upsert_average_line.format(security_code=Utils.quotes_surround(security_code),
                                                    the_date=Utils.quotes_surround(list_data[0].strftime('%Y-%m-%d')),
                                                    exchange_code=Utils.quotes_surround(exchange_code),
                                                    ma=ma,
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
                        if len_result == ma:
                            progress = Utils.base_round_zero(1 * 100, 2)
                        else:
                            progress = Utils.base_round_zero(add_up / (len_result - ma + 1) * 100, 2)

                        progress_log_list = Utils.deepcopy_list(self.log_list)
                        progress_log_list.append(self.get_method_name())
                        progress_log_list.append('ma')
                        progress_log_list.append(ma)
                        progress_log_list.append('security_code')
                        progress_log_list.append(security_code)
                        progress_log_list.append('exchange_code')
                        progress_log_list.append(exchange_code)
                        progress_log_list.append('progress')
                        progress_log_list.append(add_up)
                        progress_log_list.append((len_result - ma + 1))
                        progress_log_list.append(process_line)
                        progress_log_list.append(progress)
                        self.logger.base_log(progress_log_list)
                    else:
                        if upsert_sql is not None:
                            upsert_sql_list.append(upsert_sql)
                    i += 1

                # 处理最后一批security_code的更新语句
                if len(upsert_sql_list) > 0:
                    self.dbService.insert_many(upsert_sql_list)
                    process_line += '='
                if len_result == ma:
                    progress = Utils.base_round_zero(1 * 100, 2)
                else:
                    progress = Utils.base_round_zero(add_up / (len_result - ma + 1) * 100, 2)

                progress_log_list = Utils.deepcopy_list(self.log_list)
                progress_log_list.append(self.get_method_name())
                progress_log_list.append('ma')
                progress_log_list.append(ma)
                progress_log_list.append('security_code')
                progress_log_list.append(security_code)
                progress_log_list.append('exchange_code')
                progress_log_list.append(exchange_code)
                progress_log_list.append('progress')
                progress_log_list.append(add_up)
                progress_log_list.append((len_result - ma + 1))
                progress_log_list.append(process_line)
                progress_log_list.append(progress)
                self.logger.base_log(progress_log_list)
        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            error_log_list = Utils.deepcopy_list(self.log_list)
            error_log_list.append(self.get_method_name())
            error_log_list.append('ma')
            error_log_list.append(ma)
            error_log_list.append('security_code')
            error_log_list.append(security_code)
            error_log_list.append('exchange_code')
            error_log_list.append(exchange_code)
            error_log_list.append(exc_type)
            error_log_list.append(exc_value)
            error_log_list.append(exc_traceback)
            self.logger.base_log(error_log_list, logging.ERROR)

        end_log_list = Utils.deepcopy_list(self.log_list)
        end_log_list.append(self.get_method_name())
        end_log_list.append('ma')
        end_log_list.append(ma)
        end_log_list.append('security_code')
        end_log_list.append(security_code)
        end_log_list.append('exchange_code')
        end_log_list.append(exchange_code)
        end_log_list.append('【end】')
        self.logger.base_log(end_log_list)

    def analysis_average_line(self, ma, temp_line_tuple, security_code, exchange_code, previous_data):

        # 前一ma日均收盘价，默认值为0，便于写入数据库
        close_pre_avg = 0
        # 前一ma日均成交额(元)
        amount_pre_avg = 0
        # 前一ma日均成交量(手)
        vol_pre_avg = 0
        # 前一ma日均成交价
        price_pre_avg = 0
        # close_pre_avg, amount_pre_avg, vol_pre_avg, price_pre_avg
        if previous_data is not None and len(previous_data) > 0:
            close_pre_avg = previous_data[0][0]
            amount_pre_avg = previous_data[0][1]
            vol_pre_avg = previous_data[0][2]
            price_pre_avg = previous_data[0][3]

        # temp_line_tuple中的数据为the_date, close, amount, vol
        # 当日the_date为正序排序最后一天的the_date，第一个元素
        the_date = temp_line_tuple[ma - 1][0]
        # 将元组元素转换为列表元素
        # temp_items = [item for item in temp_line_tuple[0:]]

        # 当日收盘价=正序排序最后一天的收盘价，最后一个元素的第2个元素
        close = temp_line_tuple[ma - 1][1]
        # ma日均收盘价=sum(前ma日(含)的收盘价)/ma
        close_list = [close for close in [item[1] for item in temp_line_tuple]]
        close_avg = Utils.base_round_zero(Utils.average_zero(close_list), 2)
        # 如果收盘ma日均价为None，则为异常数据，价格不可能为0
        # if close_avg is None:
        #     close_avg = self.base_round_zero(Decimal(0), 2)
        # ma日均收盘价涨跌幅=(ma日均收盘价 - 前一ma日均收盘价)/前一ma日均收盘价 * 100
        # 默认值为0
        close_avg_chg = 0
        # if close_pre_avg is not None and close_pre_avg != Decimal(0):
        #     close_avg_chg = self.base_round_zero(self.division_zero((close_avg - close_pre_avg), close_pre_avg) * 100, 2)
        close_avg_chg = Utils.base_round_zero(Utils.division_zero((close_avg - close_pre_avg), close_pre_avg) * 100, 2)

        # 当日成交额
        amount = temp_line_tuple[ma - 1][2]
        # ma日均成交额=sum(前ma日(含)的成交额)/ma
        amount_list = [amount for amount in [item[2] for item in temp_line_tuple]]
        amount_avg = Utils.base_round_zero(Utils.average_zero(amount_list), 2)
        # if amount_avg is None :
        #     amount_avg = Decimal(0)
        # ma日均成交额涨跌幅=(ma日均成交额 - 前一ma日均成交额)/前一ma日均成交额 * 100
        # 默认值为0
        amount_avg_chg = 0
        # if amount_pre_avg is not None and amount_pre_avg != Decimal(0):
        #     amount_avg_chg = self.base_round_zero(self.division_zero((amount_avg - amount_pre_avg), amount_pre_avg) * 100, 2)
        amount_avg_chg = Utils.base_round_zero(Utils.division_zero((amount_avg - amount_pre_avg), amount_pre_avg) * 100, 2)

        # 当日成交量
        vol = temp_line_tuple[ma - 1][3]
        # ma日均成交量=sum(前ma日(含)的成交量)/ma
        vol_list = [vol for vol in [item[3] for item in temp_line_tuple]]
        vol_avg = Utils.base_round_zero(Utils.average_zero(vol_list), 2)
        # if vol_avg is None:
        #     vol_avg = Decimal(0)
        # ma日均成交量涨跌幅=(ma日均成交量 - 前一ma日均成交量)/前一ma日均成交量 * 100
        vol_avg_chg = 0
        # if vol_pre_avg is not None and vol_pre_avg != Decimal(0):
        #     vol_avg_chg = self.base_round_zero(self.division_zero((vol_avg - vol_pre_avg), vol_pre_avg) * 100, 2)
        vol_avg_chg = Utils.base_round_zero(Utils.division_zero((vol_avg - vol_pre_avg), vol_pre_avg) * 100, 2)

        # ma日均成交价=sum(前ma日(含)的成交额)/sum(ma日(含)的成交量)
        price_avg = Utils.base_round_zero(Utils.division_zero(Utils.sum_zero(amount_list), Utils.sum_zero(vol_list)), 2)
        # if price_avg is None:
        #     price_avg = Decimal(0)
        # ma日均成交价涨跌幅=(ma日均成交价 - 前一ma日均成交价)/前一ma日均成交价 * 100
        price_avg_chg = 0
        # if price_pre_avg is not None and price_pre_avg != Decimal(0):
        #     price_avg_chg = self.base_round_zero(self.division_zero((price_avg - price_pre_avg), price_pre_avg) * 100, 2)
        price_avg_chg = Utils.base_round_zero(Utils.division_zero((price_avg - price_pre_avg), price_pre_avg) * 100, 2)

        # 日金钱流向涨跌幅=日成交额/ma日(含)均成交额 * 100
        amount_flow_chg = Utils.base_round_zero(Utils.division_zero(amount, amount_avg), 2)
        # if amount_flow_chg is None:
        #     amount_flow_chg = Decimal(0)

        # 日成交量流向涨跌幅=日成交量/ma日(含)均成交量 * 100
        vol_flow_chg = Utils.base_round_zero(Utils.division_zero(vol, vol_avg), 2)
        # if vol_flow_chg is None:
        #     vol_flow_chg = Decimal(0)

        return [the_date,
                close, close_avg, close_pre_avg, close_avg_chg,
                amount, amount_avg, amount_pre_avg, amount_avg_chg,
                vol, vol_avg, vol_pre_avg, vol_avg_chg,
                price_avg, price_pre_avg, price_avg_chg,
                amount_flow_chg, vol_flow_chg]

    def processing_average_line_avg(self, ma, security_code, exchange_code):
        start_log_list = Utils.deepcopy_list(self.log_list)
        start_log_list.append(self.get_method_name())
        start_log_list.append('ma')
        start_log_list.append(ma)
        start_log_list.append('security_code')
        start_log_list.append(security_code)
        start_log_list.append('exchange_code')
        start_log_list.append(exchange_code)
        start_log_list.append('【start】...')
        self.logger.base_log(start_log_list)

        if self.is_reset:
            decline_ma_the_date = None
        else:
            average_line_avg_max_the_date = self.dbService.get_average_line_avg_max_the_date(ma, security_code, exchange_code)
            decline_ma_the_date = self.dbService.get_average_line_avg_decline_max_the_date(ma, average_line_avg_max_the_date)
        result = self.dbService.get_average_line(ma, security_code, exchange_code, decline_ma_the_date)
        len_result = len(result)
        if len_result < ma:
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
                    if (i + ma) > len_result:
                        add_up -= 1
                        break
                    temp_line_tuple = result[i:(i + ma)]

                    # 返回值list_data list [the_date,
                    # close_avg_chg_avg, amount_avg_chg_avg, vol_avg_chg_avg,
                    # price_avg_chg_avg, amount_flow_chg_avg, vol_flow_chg_avg]
                    list_data = self.analysis_average_line_avg(ma, temp_line_tuple, security_code, exchange_code)
                    upsert_sql = StockOneStepBusinessService.upsert_average_line_avg.format(security_code=Utils.quotes_surround(security_code),
                                                    the_date=Utils.quotes_surround(list_data[0].strftime('%Y-%m-%d')),
                                                    exchange_code=Utils.quotes_surround(exchange_code),
                                                    ma=ma,
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
                        if len_result == ma:
                            progress = Utils.base_round(1 * 100, 2)
                        else:
                            progress = Utils.base_round(add_up / (len_result - ma + 1) * 100, 2)

                        progress_log_list = Utils.deepcopy_list(self.log_list)
                        progress_log_list.append(self.get_method_name())
                        progress_log_list.append('ma')
                        progress_log_list.append(ma)
                        progress_log_list.append('security_code')
                        progress_log_list.append(security_code)
                        progress_log_list.append('exchange_code')
                        progress_log_list.append(exchange_code)
                        progress_log_list.append('progress')
                        progress_log_list.append(add_up)
                        progress_log_list.append((len_result - ma + 1))
                        progress_log_list.append(process_line)
                        progress_log_list.append(progress)
                        self.logger.base_log(progress_log_list)
                    else:
                        if upsert_sql is not None:
                            upsert_sql_list.append(upsert_sql)
                    i += 1

                # 处理最后一批security_code的更新语句
                if len(upsert_sql_list) > 0:
                    self.dbService.insert_many(upsert_sql_list)
                    process_line += '='
                if len_result == ma:
                    progress = Utils.base_round(1 * 100, 2)
                else:
                    progress = Utils.base_round(add_up / (len_result - ma + 1) * 100, 2)

                progress_log_list = Utils.deepcopy_list(self.log_list)
                progress_log_list.append(self.get_method_name())
                progress_log_list.append('ma')
                progress_log_list.append(ma)
                progress_log_list.append('security_code')
                progress_log_list.append(security_code)
                progress_log_list.append('exchange_code')
                progress_log_list.append(exchange_code)
                progress_log_list.append('progress')
                progress_log_list.append(add_up)
                progress_log_list.append((len_result - ma + 1))
                progress_log_list.append(process_line)
                progress_log_list.append(progress)
                self.logger.base_log(progress_log_list)
        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            error_log_list = Utils.deepcopy_list(self.log_list)
            error_log_list.append(self.get_method_name())
            error_log_list.append('ma')
            error_log_list.append(ma)
            error_log_list.append('security_code')
            error_log_list.append(security_code)
            error_log_list.append('exchange_code')
            error_log_list.append(exchange_code)
            error_log_list.append(exc_type)
            error_log_list.append(exc_value)
            error_log_list.append(exc_traceback)
            self.logger.base_log(error_log_list, logging.ERROR)

        end_log_list = Utils.deepcopy_list(self.log_list)
        end_log_list.append(self.get_method_name())
        end_log_list.append('ma')
        end_log_list.append(ma)
        end_log_list.append('security_code')
        end_log_list.append(security_code)
        end_log_list.append('exchange_code')
        end_log_list.append(exchange_code)
        end_log_list.append('【end】')
        self.logger.base_log(end_log_list)

    def analysis_average_line_avg(self, ma, temp_line_tuple, security_code, exchange_code):

        # temp_line_tuple中的数据为the_date, close_avg_chg, amount_avg_chg,
        # vol_avg_chg, price_avg_chg, amount_flow_chg, vol_flow_chg
        # 当日the_date为正序排序最后一天的the_date，第一个元素
        the_date = temp_line_tuple[ma - 1][0]
        # 将元组元素转换为列表元素
        # temp_items = [item for item in temp_line_tuple[0:]]

        # ma日均收盘价涨跌幅均=sum(前ma日(含)均收盘价涨跌幅)/ma
        close_avg_chg_list = [close_avg_chg for close_avg_chg in [item[1] for item in temp_line_tuple]]
        close_avg_chg_avg = Utils.base_round(Utils.average(close_avg_chg_list), 2)

        # ma日均成交额涨跌幅均=sum(前ma日(含)均成交额涨跌幅)/ma
        amount_avg_chg_list = [amount_avg_chg for amount_avg_chg in [item[2] for item in temp_line_tuple]]
        amount_avg_chg_avg = Utils.base_round(Utils.average(amount_avg_chg_list), 2)

        # ma日均成交量涨跌幅均=sum(前ma日(含)均成交量涨跌幅)/ma
        vol_avg_chg_list = [vol_avg_chg for vol_avg_chg in [item[3] for item in temp_line_tuple]]
        vol_avg_chg_avg = Utils.base_round(Utils.average(vol_avg_chg_list), 2)

        # ma日均成交价涨跌幅均=sum(前ma日(含)均成交价涨跌幅)/ma
        price_avg_chg_list = [price_avg_chg for price_avg_chg in [item[4] for item in temp_line_tuple]]
        price_avg_chg_avg = Utils.base_round(Utils.average(price_avg_chg_list), 2)

        # 日金钱流向涨跌幅均=sum(前ma日(含)金钱流向涨跌幅)/ma
        amount_flow_chg_list = [amount_flow_chg for amount_flow_chg in [item[5] for item in temp_line_tuple]]
        amount_flow_chg_avg = Utils.base_round(Utils.average(amount_flow_chg_list), 2)

        # 日成交量流向涨跌幅均=sum(前ma日(含)成交量流向涨跌幅)/ma
        vol_flow_chg_list = [vol_flow_chg for vol_flow_chg in [item[5] for item in temp_line_tuple]]
        vol_flow_chg_avg = Utils.base_round(Utils.average(vol_flow_chg_list), 2)


        return [the_date,
                close_avg_chg_avg, amount_avg_chg_avg, vol_avg_chg_avg,
                price_avg_chg_avg, amount_flow_chg_avg, vol_flow_chg_avg]

