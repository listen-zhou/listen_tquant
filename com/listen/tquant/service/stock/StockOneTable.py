# coding: utf-8
import datetime
import inspect

import tquant as tt
import numpy

from com.listen.tquant.dbservice.Service import DbService
from com.listen.tquant.service.stock.UtilService import UtilService
from com.listen.tquant.utils.Utils import Utils
import traceback
import time

class StockOneTable():
    """
    股票历史行情及衍生数据一条龙处理服务
    """
    dbService = DbService()

    def get_classs_name(self):
        return self.__class__.__name__

    def get_method_name(self):
        return inspect.stack()[1][3]

    def __init__(self, security_code, is_reset):
        self.security_code = security_code
        self.is_reset = is_reset
        self.mas = [3, 5, 10]
        log_list = [Utils.get_now(), Utils.get_info(), self.get_classs_name(), self.security_code,
                    self.get_method_name(), 'mas', self.mas]
        Utils.print_log(log_list)

    def processing_single_security_code(self):
        log_list = [Utils.get_now(), Utils.get_info(), self.get_classs_name(), self.security_code,
                    self.get_method_name()]
        Utils.print_log(log_list)
        """
        单只股票处理方法
        :return: 
        """
        # 股票日K数据处理方法
        self.processing_day_kline()
        # 股票日K数据处理后有关计算的方法
        self.is_reset = True
        self.procesing_day_kline_after()
        self.is_reset = False
        self.processing_real_time_kline()

    #################################################################################################

    def processing_day_kline(self):
        """
        股票日K数据处理，分全量还是增量
        :return: 
        """
        try:
            if self.is_reset:
                max_the_date = datetime.datetime.now().replace(year=1970, month=1, day=1)
            else:
                max_the_date = self.get_day_kline_maxthedate()
            diff_days = Utils.diff_days(max_the_date)
            log_list = [Utils.get_now(), Utils.get_info(), self.get_classs_name(), self.security_code,
                        self.get_method_name(), '日K最大交易日', max_the_date, '距今', Utils.format_date(datetime.date.today()), '差', diff_days, '天']
            Utils.print_log(log_list)
            if diff_days is None:
                result = tt.get_all_daybar(self.security_code, 'qfq')
            elif diff_days > 0:
                result = tt.get_last_n_daybar(self.security_code, diff_days, 'qfq')
            else:
                result = tt.get_last_n_daybar(self.security_code, 7, 'qfq')

            if result.empty == False:
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
                        dict_data = self.analysis_columns_day_kline(result, idx)
                        if dict_data is not None:
                            the_date = dict_data['the_date']
                            is_exist = self.get_day_kline_exist(the_date)
                            upsert_sql = UtilService.get_day_kline_upsertsql(self.security_code, dict_data, is_exist)
                            upsert_sql_list.append(upsert_sql)
                        # 批量(100)提交数据更新
                        if len(upsert_sql_list) == 200:
                            self.dbService.insert_many(upsert_sql_list)
                            process_line += '='
                            upsert_sql_list = []
                            progress = Utils.base_round(Utils.division_zero(add_up, len_indexes) * 100, 2)

                            log_list = [Utils.get_now(), Utils.get_info(), self.get_classs_name(), self.security_code,
                                        self.get_method_name(), '处理进度', add_up, len_indexes, process_line, str(progress) + '%']
                            Utils.print_log(log_list)
                    # 处理最后一批security_code的更新语句
                    if len(upsert_sql_list) > 0:
                        self.dbService.insert_many(upsert_sql_list)
                        process_line += '='
                    progress = Utils.base_round(Utils.division_zero(add_up, len(indexes_values)) * 100, 2)

                    log_list = [Utils.get_now(), Utils.get_info(), self.get_classs_name(), self.security_code,
                                self.get_method_name(), '处理进度', add_up, len_indexes, process_line, str(progress) + '%']
                    Utils.print_log(log_list)
                else:
                    log_list = [Utils.get_now(), Utils.get_warn(), self.get_classs_name(), self.security_code,
                                self.get_method_name(), '【日K DataFrame索引为空】']
                    Utils.print_log(log_list)
            else:
                log_list = [Utils.get_now(), Utils.get_warn(), self.get_classs_name(), self.security_code,
                            self.get_method_name(), '【日K DataFrame为空】']
                Utils.print_log(log_list)
        except Exception:
            log_list = [Utils.get_now(), Utils.get_info(), self.get_classs_name(), self.security_code,
                        self.get_method_name(), traceback.format_exc()]
            Utils.print_log(log_list)

    def get_day_kline_exist(self, the_date):
        sql = "select count(*) from tquant_stock_history_quotation " \
              "where security_code = {security_code} and the_date = {the_date} "
        sql = sql.format(security_code=Utils.quotes_surround(self.security_code),
                         the_date=Utils.quotes_surround(the_date))
        size = self.dbService.query(sql)
        if size is not None:
            size = size[0][0]
            if size == 1:
                return True
            return False

    def get_day_kline_maxthedate(self):
        sql = "select the_date from tquant_stock_history_quotation " \
              "where security_code = {security_code} " \
              "order by the_date desc limit 1 ".format(security_code=Utils.quotes_surround(self.security_code))
        the_date = self.dbService.query(sql)
        if the_date is not None and len(the_date) > 0:
            return the_date[0][0]
        return None

    def analysis_columns_day_kline(self, day_kline, idx):
        """
        股票日K数据处理方法
        :param day_kline: 日K的DataFrame
        :param idx: DataFrame的索引
        :return: 
        """
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

            return {'the_date': the_date, 'amount': amount,
                    'vol': vol, 'open': open, 'high': high, 'low': low, 'close': close}
        except Exception:
            log_list = [Utils.get_now(), Utils.get_info(), self.get_classs_name(), self.security_code]
            log_list.append(self.get_method_name())
            log_list.append(traceback.format_exc())
            Utils.print_log(log_list)
            return None
    #############################################################################################################

    def procesing_day_kline_after(self):
        """
        日K数据入库后计算涨跌幅，均线，均值等数据，并入库
        :return: 
        """
        # 股票日K涨跌幅处理方法
        log_list = [Utils.get_now(), Utils.get_info(), self.get_classs_name(), self.security_code,
                    self.get_method_name()]
        Utils.print_log(log_list)

        self.processing_day_kline_chg()
        if self.mas is not None and len(self.mas) > 0:
            for ma in self.mas:
                # 股票均线数据处理方法
                self.processing_ma(ma)
            self.processing_ma_10_diff()

    def processing_ma_10_diff(self):
        try:
            ma = 10
            if self.is_reset:
                min_the_date = datetime.datetime.now().replace(year=1970, month=1, day=1)
            else:
                min_the_date = self.get_day_kline_ma_10_diff_maxthedate()
            log_list = [Utils.get_now(), Utils.get_warn(), self.get_classs_name(), self.security_code,
                        self.get_method_name(), 'ma', ma, 'MA10 diff最大交易日(前10日)', Utils.format_date(min_the_date)]
            Utils.print_log(log_list)
            result = self.get_all_day_kline(min_the_date)
            if result is not None and len(result) > 0:
                update_list = []
                process_line = ''
                add_up = ma - 1
                dict_pre_data = None
                for i in range(len(result)):
                    if (i + ma) > len(result):
                        break
                    add_up += 1
                    temp_data = result[i:i + ma]
                    if dict_pre_data is None:
                        dict_pre_data = self.get_pre_day_kline_ma_10_avg_chg_avg(temp_data[len(temp_data) - 1][0])
                        if dict_pre_data is None:
                            dict_pre_data = {'price_avg_chg_10_avg_pre': 0}
                        print(self.security_code, ma, min_the_date, 'dict_pre_data', dict_pre_data)
                    dict_data = self.get_calculate_ma_10_chg_diff(temp_data, dict_pre_data)
                    dict_pre_data['price_avg_chg_10_avg_pre'] = dict_data['price_avg_chg_10_avg']
                    the_date = temp_data[len(temp_data) - 1][0]
                    update_sql = self.update_ma_10_chg_diff(dict_data, the_date)
                    print(update_sql)
                    update_list.append(update_sql)
                    if len(update_list) == 200:
                        self.dbService.insert_many(update_list)
                        process_line += '='
                        progress = Utils.base_round(Utils.division_zero(add_up, len(result)) * 100, 2)
                        log_list = [Utils.get_now(), Utils.get_info(), self.get_classs_name(), self.security_code,
                                    self.get_method_name(), 'ma', ma, '处理进度', add_up, len(result), process_line,
                                    str(progress) + '%']
                        Utils.print_log(log_list)
                        update_list = []
                if len(update_list) > 0:
                    self.dbService.insert_many(update_list)
                process_line += '='
                progress = Utils.base_round(Utils.division_zero(add_up, len(result)) * 100, 2)
                log_list = [Utils.get_now(), Utils.get_info(), self.get_classs_name(), self.security_code,
                            self.get_method_name(),  'ma', ma, '处理进度', add_up, len(result), process_line,
                            str(progress) + '%']
                Utils.print_log(log_list)
        except Exception:
            log_list = [Utils.get_now(), Utils.get_info(), self.get_classs_name(), self.security_code,
                        self.get_method_name(), traceback.format_exc()]
            Utils.print_log(log_list)

    def processing_ma(self, ma):
        # ma对应的字段名
        price_avg_ma = 'price_avg_' + str(ma)
        price_avg_chg_ma = 'price_avg_chg_' + str(ma)
        # 确认已经处理过的最大交易日
        try:
            if self.is_reset:
                min_the_date = datetime.datetime.now().replace(year=1970, month=1, day=1)
            else:
                min_the_date = self.get_day_kline_ma_maxthedate(price_avg_ma, ma)
            log_list = [Utils.get_now(), Utils.get_warn(), self.get_classs_name(), self.security_code,
                        self.get_method_name(), 'ma', ma, 'MA均线最大交易日(前ma日)', Utils.format_date(min_the_date)]
            Utils.print_log(log_list)

            result = self.get_all_day_kline(min_the_date)
            if result is not None and len(result) > 0:
                update_list = []
                process_line = ''
                add_up = ma - 1
                dict_pre_data = None
                for i in range(len(result)):
                    if (i + ma) > len(result):
                        break
                    add_up += 1
                    temp_data = result[i:i + ma]
                    if dict_pre_data is None:
                        dict_pre_data = self.get_pre_day_kline_ma(temp_data[len(temp_data)-1][0], price_avg_ma, price_avg_chg_ma)
                        if dict_pre_data is None:
                            dict_pre_data = {'price_avg_ma_pre': 0, 'price_avg_chg_ma_pre': 0}
                    dict_data = self.get_calculate_ma_chg(temp_data, dict_pre_data)
                    dict_pre_data['price_avg_ma_pre'] = dict_data['price_avg_ma']
                    dict_pre_data['price_avg_chg_ma_pre'] = dict_data['price_avg_chg_ma']
                    the_date = temp_data[len(temp_data) - 1][0]
                    update_sql = self.update_ma_chg(price_avg_ma, price_avg_chg_ma, dict_data, the_date)
                    update_list.append(update_sql)
                    if len(update_list) == 200:
                        self.dbService.insert_many(update_list)
                        process_line += '='
                        progress = Utils.base_round(Utils.division_zero(add_up, len(result)) * 100, 2)
                        log_list = [Utils.get_now(), Utils.get_info(), self.get_classs_name(), self.security_code,
                                    self.get_method_name(), 'ma', ma, '处理进度', add_up, len(result), process_line,
                                    str(progress) + '%']
                        Utils.print_log(log_list)
                        update_list = []
                if len(update_list) > 0:
                    self.dbService.insert_many(update_list)
                process_line += '='
                progress = Utils.base_round(Utils.division_zero(add_up, len(result)) * 100, 2)
                log_list = [Utils.get_now(), Utils.get_info(), self.get_classs_name(), self.security_code,
                            self.get_method_name(),  'ma', ma, '处理进度', add_up, len(result), process_line,
                            str(progress) + '%']
                Utils.print_log(log_list)
        except Exception:
            log_list = [Utils.get_now(), Utils.get_info(), self.get_classs_name(), self.security_code,
                        self.get_method_name(), 'ma', ma, traceback.format_exc()]
            Utils.print_log(log_list)


    def update_ma_chg(self, price_avg_ma, price_avg_chg_ma, dict_data, the_date):
        sql = "update tquant_stock_history_quotation " \
              "set "
        if dict_data['money_flow'] is not None:
            sql += "money_flow = {money_flow}, "
            sql = sql.format(money_flow=dict_data['money_flow'])
        if dict_data['close_10_price_avg_chg'] is not None:
            sql += "close_10_price_avg_chg = {close_10_price_avg_chg}, "
            sql = sql.format(close_10_price_avg_chg=dict_data['close_10_price_avg_chg'])
        sql += price_avg_ma + " = {price_avg_ma}, "
        sql += price_avg_chg_ma + " = {price_avg_chg_ma} "
        sql += "where security_code = {security_code} and the_date = {the_date} "

        sql = sql.format(price_avg_ma=dict_data['price_avg_ma'],
                         price_avg_chg_ma=dict_data['price_avg_chg_ma'],
                         security_code=Utils.quotes_surround(self.security_code),
                         the_date=Utils.quotes_surround(Utils.format_date(the_date))
                         )
        return sql

    def update_ma_10_chg_diff(self, dict_data, the_date):
        sql = "update tquant_stock_history_quotation " \
              "set price_avg_chg_10_avg = {price_avg_chg_10_avg}, " \
              "price_avg_chg_10_avg_diff = {price_avg_chg_10_avg_diff} " \
              "where security_code = {security_code} and the_date = {the_date} "
        sql = sql.format(price_avg_chg_10_avg=dict_data['price_avg_chg_10_avg'],
                         price_avg_chg_10_avg_diff=dict_data['price_avg_chg_10_avg_diff'],
                         security_code=Utils.quotes_surround(self.security_code),
                         the_date=Utils.quotes_surround(Utils.format_date(the_date))
                         )
        return sql

    def get_pre_day_kline_ma(self, the_date, price_avg_ma, price_avg_chg_ma):
        sql = "select " + price_avg_ma + ", " + price_avg_chg_ma \
              + " from tquant_stock_history_quotation " \
              "where security_code = {security_code} and the_date < {the_date} " \
              "order by the_date desc limit 1"
        sql = sql.format(security_code=Utils.quotes_surround(self.security_code),
                         the_date=Utils.quotes_surround(Utils.format_date(the_date)))
        result = self.dbService.query(sql)
        if result is not None and len(result) > 0:
            price_avg = result[0][0]
            price_avg_chg = result[0][1]
            return {'price_avg_ma_pre': price_avg, 'price_avg_chg_ma_pre': price_avg_chg}
        return None

    def get_pre_day_kline_ma_10_avg_chg_avg(self, the_date):
        sql = "select price_avg_chg_10_avg from tquant_stock_history_quotation " \
              "where security_code = {security_code} and the_date < {the_date} " \
              "order by the_date desc limit 1"
        sql = sql.format(security_code=Utils.quotes_surround(self.security_code),
                         the_date=Utils.quotes_surround(Utils.format_date(the_date)))
        result = self.dbService.query(sql)
        if result is not None and len(result) > 0:
            price_avg_chg_10_avg = result[0][0]
            return {'price_avg_chg_10_avg_pre': price_avg_chg_10_avg}
        return None


    def get_calculate_ma_chg(self, temp_data, dict_pre_data):
        """
        sql = "select the_date, amount, vol, open, high, low, close " \
              "from tquant_stock_history_quotation " \
              "where security_code = {security_code} " \
              "and the_date >= {min_the_date} " \
              "order by the_date asc "
        """
        price_avg_ma_pre = dict_pre_data['price_avg_ma_pre']
        price_avg_chg_ma_pre = dict_pre_data['price_avg_chg_ma_pre']

        if price_avg_ma_pre is None:
            price_avg_ma_pre = 0
        if price_avg_chg_ma_pre is None:
            price_avg_chg_ma_pre = 0
        amount_list = [amount for amount in [item[1] for item in temp_data]]
        vol_list = [vol for vol in [item[2] for item in temp_data]]
        price_avg_ma = Utils.base_round_zero(Utils.division_zero(Utils.sum_zero(amount_list), Utils.sum_zero(vol_list)), 2)
        price_avg_chg_ma = Utils.base_round_zero(Utils.division_zero((price_avg_ma - price_avg_ma_pre), price_avg_ma_pre) * 100, 2)
        amount = temp_data[len(temp_data) - 1][1]
        money_flow = None
        close_10_price_avg_chg = None
        if len(temp_data) == 10:
            amount_avg = Utils.base_round(Utils.division_zero(Utils.sum_zero(amount_list), 10), 2)
            money_flow = Utils.base_round(Utils.division_zero(amount, amount_avg) * 100, 2)
            close = temp_data[len(temp_data)-1][6]
            close_10_price_avg_chg = Utils.base_round_zero(Utils.division_zero(close - price_avg_ma, price_avg_ma) * 100, 2)
        return {'price_avg_ma': price_avg_ma, 'price_avg_chg_ma': price_avg_chg_ma,
                'money_flow': money_flow, 'close_10_price_avg_chg': close_10_price_avg_chg}

    def get_calculate_ma_10_chg_diff(self, temp_data, dict_pre_data):
        """
        sql = "select the_date, amount, vol, open, high, low, close, price_avg_chg_10 " \
              "from tquant_stock_history_quotation " \
              "where security_code = {security_code} " \
              "and the_date >= {min_the_date} " \
              "order by the_date asc "
        """
        price_avg_chg_10_avg_pre = dict_pre_data['price_avg_chg_10_avg_pre']

        if price_avg_chg_10_avg_pre is None:
            price_avg_chg_10_avg_pre = 0
        price_avg_chg_10_list = [price_avg_chg_10 for price_avg_chg_10 in [item[7] for item in temp_data]]
        price_avg_chg_10_avg = Utils.base_round_zero(Utils.division_zero(Utils.sum_zero(price_avg_chg_10_list), len(temp_data)), 2)
        price_avg_chg_10_avg_diff = price_avg_chg_10_avg - price_avg_chg_10_avg_pre

        return {'price_avg_chg_10_avg': price_avg_chg_10_avg, 'price_avg_chg_10_avg_diff': price_avg_chg_10_avg_diff}

    def get_day_kline_ma_maxthedate(self, price_avg_ma, ma):
        sql = "select the_date from tquant_stock_history_quotation " \
              "where security_code = {security_code} " \
              "and " + price_avg_ma + " is not null " \
              "order by the_date desc limit {ma}"
        the_dates = self.dbService.query(sql.format(security_code=Utils.quotes_surround(self.security_code), ma=ma))
        if the_dates is not None and len(the_dates) > 0:
            the_date = the_dates[len(the_dates) - 1][0]
            return the_date
        return None

    def get_day_kline_ma_10_diff_maxthedate(self):
        sql = "select the_date from tquant_stock_history_quotation " \
              "where security_code = {security_code} " \
              "and price_avg_chg_10_avg is not null " \
              "order by the_date desc limit 10 "
        the_dates = self.dbService.query(sql.format(security_code=Utils.quotes_surround(self.security_code)))
        if the_dates is not None and len(the_dates) > 0:
            the_date = the_dates[len(the_dates)-1][0]
            return the_date
        return None

    def processing_real_time_kline(self):
        """
        处理单只股票的实时行情
        :return: 
        """
        try:
            current_date = datetime.datetime.now().replace(microsecond=0)
            start_date = current_date.replace(hour=9, minute=30, second=0, microsecond=0)
            end_date = current_date.replace(hour=15, minute=0, second=0, microsecond=0)
            log_list = [Utils.get_now(), Utils.get_info(), self.get_classs_name(), self.security_code,
                        self.get_method_name(), '当日全部5分钟实时行情 开始时间', start_date, '拉取时间', current_date,
                        '结束时间', end_date]
            Utils.print_log(log_list)
            # if current_date <= end_date and current_date >= start_date:
            if True:
                # 5分钟K的实时行情
                day_kline = tt.get_stock_bar(self.security_code, 1)
                # print('day_kline', day_kline)
                # 处理单只股票的实时行情，并入库
                self.analysis_real_time_kline(day_kline, start_date)
                # 股票日K涨跌幅处理方法
                self.procesing_day_kline_after()
        except Exception:
            log_list = [Utils.get_now(), Utils.get_info(), self.get_classs_name(), self.security_code,
                        self.get_method_name(), traceback.format_exc()]
            Utils.print_log(log_list)


    def analysis_real_time_kline(self, day_kline, start_date):
        """
        解析单只股票的实时行情，并入库
        :param day_kline: 
        :param start_date: 
        :return: 
        """
        try:
            if day_kline.empty == False:
                indexes_values = day_kline.index.values
                if indexes_values is None or len(indexes_values) == 0:
                    log_list = [Utils.get_now(), Utils.get_warn(), self.get_classs_name(), self.security_code,
                                self.get_method_name(), '当日全部5分钟实时行情 开始时间', start_date, '【行情为空】']
                    Utils.print_log(log_list)
                    return
                add_up = 0
                process_line = '='
                len_indexes = len(indexes_values)
                the_date = None
                high_max = None
                low_min = None
                open_first = None
                close_last = None
                total_amount = 0
                total_vol = 0
                for idx in indexes_values:
                    add_up += 1
                    idx_datetime = datetime.datetime.utcfromtimestamp(idx.astype('O') / 1e9)
                    # 由于第三方接口返回的数据是最近1000个5分钟K，所以需要剔除不是今天的数据
                    if idx_datetime >= start_date:
                        amount = day_kline.at[idx, 'amount']
                        if isinstance(amount, numpy.ndarray) and amount.size > 1:
                            amount = amount.tolist()[0]
                        amount = Utils.base_round(amount, 2)
                        total_amount += amount

                        vol = day_kline.at[idx, 'vol']
                        if isinstance(vol, numpy.ndarray) and vol.size > 1:
                            vol = vol.tolist()[0]
                        vol = Utils.base_round(vol, 2)
                        total_vol += Utils.base_round(vol, 2)

                        high = day_kline.at[idx, 'high']
                        if isinstance(high, numpy.ndarray) and high.size > 1:
                            high = high.tolist()[0]
                        high = Utils.base_round(high, 2)
                        if high_max is None:
                            high_max = high
                        elif high > high_max:
                            high_max = high

                        low = day_kline.at[idx, 'low']
                        if isinstance(low, numpy.ndarray) and low.size > 1:
                            low = low.tolist()[0]
                        low = Utils.base_round(low, 2)
                        if low_min is None:
                            low_min = low
                        elif low < low_min:
                            low_min = low

                        open = day_kline.at[idx, 'open']
                        if isinstance(open, numpy.ndarray) and open.size > 1:
                            open = open.tolist()[0]
                        open = Utils.base_round(open, 2)
                        if open_first is None:
                            open_first = open

                        close = day_kline.at[idx, 'close']
                        if isinstance(close, numpy.ndarray) and close.size > 1:
                            close = close.tolist()[0]
                        close = Utils.base_round(close, 2)
                        close_last = close

                        the_date = idx_datetime
                    if add_up % 200 == 0:
                        process_line += '='
                        progress = Utils.base_round(Utils.division_zero(add_up, len_indexes) * 100, 2)
                        log_list = [Utils.get_now(), Utils.get_info(), self.get_classs_name(), self.security_code,
                                    self.get_method_name(), '处理进度', add_up, len_indexes, process_line,
                                    str(progress) + '%']
                        Utils.print_log(log_list)
                process_line += '='
                progress = Utils.base_round(Utils.division_zero(add_up, len_indexes) * 100, 2)
                log_list = [Utils.get_now(), Utils.get_info(), self.get_classs_name(), self.security_code,
                            self.get_method_name(), '处理进度', add_up, len_indexes, process_line,
                            str(progress) + '%']
                Utils.print_log(log_list)

                if the_date is not None:
                    total_amount = total_amount * 100
                    total_vol = total_vol * 100
                    dict_data = {'the_date': Utils.format_date(the_date),
                                 'amount': total_amount,
                                 'vol': total_vol,
                                 'open': open_first,
                                 'high': high_max,
                                 'low': low_min,
                                 'close': close_last
                                 }
                    is_exist = self.get_day_kline_exist(Utils.format_date(the_date))
                    upsert_sql = UtilService.get_day_kline_upsertsql(self.security_code, dict_data, is_exist)
                    self.dbService.insert(upsert_sql)
        except Exception:
            log_list = [Utils.get_now(), Utils.get_info(), self.get_classs_name(), self.security_code,
                        self.get_method_name(), traceback.format_exc()]
            Utils.print_log(log_list)

    # @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
    def processing_day_kline_chg(self):
        try:
            if self.is_reset:
                min_the_date = datetime.datetime.now().replace(year=1970, month=1, day=1)
            else:
                min_the_date = self.get_day_kline_chg_maxthedate()
            log_list = [Utils.get_now(), Utils.get_warn(), self.get_classs_name(), self.security_code,
                        self.get_method_name(), '涨跌幅最大交易日(前推一日)', Utils.format_date(min_the_date)]
            Utils.print_log(log_list)
            result = self.get_all_day_kline(min_the_date)
            if result is not None and len(result) > 0:
                update_list = []
                """
                sql = "select the_date, amount, vol, open, high, low, close " \
                  "from tquant_stock_history_quotation " \
                  "where security_code = {security_code} " \
                  "and the_date >= {min_the_date} " \
                  "order by the_date asc "
                  """
                process_line = ''
                add_up = 0
                dict_pre_data = None
                for i in range(len(result)):
                    add_up += 1
                    if i == len(result) - 1:
                        break
                    the_date = result[i+1][0]
                    dict_item1 = {'vol': result[i][2], 'close': result[i][6], 'amount': result[i][1], 'open': result[i][3]}
                    dict_item2 = {'vol': result[i+1][2], 'close': result[i+1][6], 'amount': result[i+1][1], 'open': result[i+1][3]}
                    if dict_pre_data is None:
                        dict_pre_data = self.get_pre_day_kline(the_date)
                        if dict_pre_data is None:
                            dict_pre_data = {'price_avg_pre': 0}
                    dict_data = UtilService.processing_day_kline_chg_calculate(dict_item1, dict_item2, dict_pre_data)
                    dict_pre_data['price_avg_pre'] = dict_data['price_avg']
                    update_sql = UtilService.get_day_kline_chg_upsertsql(self.security_code, the_date, dict_data)
                    update_list.append(update_sql)
                    if len(update_list) == 200:
                        self.dbService.insert_many(update_list)
                        process_line += '='
                        progress = Utils.base_round(Utils.division_zero(add_up, len(result)) * 100, 2)
                        log_list = [Utils.get_now(), Utils.get_info(), self.get_classs_name(), self.security_code,
                                    self.get_method_name(), '处理进度', add_up, len(result), process_line,
                                    str(progress) + '%']
                        Utils.print_log(log_list)
                        update_list = []
                if len(update_list) > 0:
                    self.dbService.insert_many(update_list)
                process_line += '='
                progress = Utils.base_round(Utils.division_zero(add_up, len(result)) * 100, 2)
                log_list = [Utils.get_now(), Utils.get_info(), self.get_classs_name(), self.security_code,
                            self.get_method_name(), '处理进度', add_up, len(result), process_line,
                            str(progress) + '%']
                Utils.print_log(log_list)
        except Exception:
            log_list = [Utils.get_now(), Utils.get_info(), self.get_classs_name(), self.security_code,
                        self.get_method_name(), traceback.format_exc()]
            Utils.print_log(log_list)

    def get_pre_day_kline(self, the_date):
        sql = "select price_avg from tquant_stock_history_quotation " \
              "where security_code = {security_code} and the_date < {the_date} " \
              "order by the_date desc limit 1"
        sql = sql.format(security_code=Utils.quotes_surround(self.security_code),
                         the_date=Utils.quotes_surround(Utils.format_date(the_date)))
        result = self.dbService.query(sql)
        if result is not None and len(result) > 0:
            price_avg = result[0][0]
            return {'price_avg_pre': price_avg}
        return None


    def get_day_kline_chg_maxthedate(self):
        sql = "select the_date from tquant_stock_history_quotation " \
              "where security_code = {security_code} " \
              "and close_chg is not null " \
              "order by the_date desc limit 2"
        the_dates = self.dbService.query(sql.format(security_code=Utils.quotes_surround(self.security_code)))
        if the_dates is not None and len(the_dates) > 0:
            the_date = the_dates[len(the_dates) - 1][0]
            return the_date
        return None

    def get_all_day_kline(self, min_the_date):
        if min_the_date is None:
            min_the_date = datetime.datetime.now().replace(year=1970, month=1, day=1)
        sql = "select the_date, amount, vol, open, high, low, close, " \
              "price_avg_chg_10 " \
              "from tquant_stock_history_quotation " \
              "where security_code = {security_code} " \
              "and the_date >= {min_the_date} " \
              "order by the_date asc "
        sql = sql.format(security_code=Utils.quotes_surround(self.security_code),
                         min_the_date=Utils.quotes_surround(Utils.format_date(min_the_date)))
        result = self.dbService.query(sql)
        return result
        # @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@


    def data_transport(self, start, size):
        sql = "select security_code, the_date, amount, vol, open, high, low, close " \
              "from tquant_stock_day_kline order by id asc limit {start}, {size}"
        sql = sql.format(start=start, size=size)
        result = self.dbService.query(sql)
        if result is not None and len(result) > 0:
            update_list =[]
            for i in range(len(result)):
                update_sql = "insert into tquant_stock_history_quotation " \
                             "(security_code, the_date, amount, vol, open, high, low, close) " \
                             "values ({security_code}, {the_date}, {amount}, {vol}, {open}, {high}, {low}, {close}) "
                update_sql = update_sql.format(security_code=Utils.quotes_surround(result[i][0]),
                                               the_date=Utils.quotes_surround(Utils.format_date(result[i][1])),
                                               amount=result[i][2],
                                               vol=result[i][3],
                                               open=result[i][4],
                                               high=result[i][5],
                                               low=result[i][6],
                                               close=result[i][7]
                                               )
                update_list.append(update_sql)
            self.dbService.insert_many(update_list)
            start += len(result)
            print('trans size', start)
            time.sleep(30)
            self.data_transport(start, size)
        else:
            print('data transport done')
            return
