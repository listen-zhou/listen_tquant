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
        log_list = [Utils.get_now(), Utils.get_info(), self.get_classs_name(), self.security_code,
                    self.get_method_name()]
        Utils.print_log(log_list)

    def processing_single_security_code(self):
        log_list = [Utils.get_now(), Utils.get_info(), self.get_classs_name(), self.security_code,
                    self.get_method_name(), self.security_code + '【【start】】']
        Utils.print_log(log_list)
        """
        单只股票处理方法
        :return: 
        """
        # 股票日K数据处理方法
        self.processing_day_kline()
        # self.processing_real_time_kline()

        log_list = [Utils.get_now(), Utils.get_info(), self.get_classs_name(), self.security_code,
                    self.get_method_name(), self.security_code + '【【end】】']
        Utils.print_log(log_list)

    def processing_section(self, section, dict_data_pre):
        """
        处理切片，分3、5、10等切片数据
        tquant_stock_history_quotation
            `security_code` VARCHAR(20) NOT NULL COMMENT '股票代码',
            `the_date` DATE NOT NULL COMMENT '交易日',
            `close` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '收盘价',
            `close_chg` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '收盘价幅',
            `close_open_chg` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '收盘价与开盘价幅',
            `open` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '开盘价',
            `open_chg` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '开盘价幅',
            `high` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '最高价',
            `high_chg` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '最高价幅',
            `low` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '最低价',
            `low_chg` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '最低价幅',
            `amount` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '交易额(元)',
            `amount_chg` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '交易额幅',
            `vol` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '交易量(手)',
            `vol_chg` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '交易量幅',
            `week_day` INT(11) NULL DEFAULT NULL COMMENT '周几',
            `price_avg_1` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '日均价',
            `price_avg_1_chg` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '日均价涨跌幅',
            `price_avg_1_chg_diff` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '日均价涨跌幅差',
            `close_price_avg_1_chg` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '收盘与日均价幅',
            `price_avg_3` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '3日均价',
            `price_avg_3_chg` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '3日均价涨跌幅',
            `price_avg_3_chg_diff` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '3日均价涨跌幅差',
            `close_price_avg_3_chg` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '收盘与3日均价幅',
            `price_avg_5` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '5日均价',
            `price_avg_5_chg` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '5日均价涨跌幅',
            `price_avg_5_chg_diff` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '5日均价涨跌幅差',
            `close_price_avg_5_chg` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '收盘与5日均价幅',
            `price_avg_10` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '10日均价',
            `price_avg_10_chg` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '10日均价涨跌幅',
            `price_avg_10_chg_diff` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '10日均价涨跌幅差',
            `close_price_avg_10_chg` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '收盘与10日均价幅',
        :param section:
                        DataFrame切片数据
                        example         amount  close  code  high   low  open        vol
                        date
                        1991-11-29     8557000   0.11     1  0.13  0.11  0.13     306200
                        1991-11-30     3962000   0.11     1  0.11  0.11  0.11     142900
                        1991-12-02    11099000   0.10     1  0.11  0.10  0.11     410500
                        1991-12-03     6521000   0.12     1  0.12  0.10  0.10     234800
                        1991-12-04     6853000   0.11     1  0.12  0.11  0.12     242700
                        1991-12-05     3458000   0.10     1  0.11  0.10  0.11     126000
                        1991-12-06     4341000   0.11     1  0.11  0.10  0.10     159400
                        1991-12-07     2218000   0.11     1  0.11  0.11  0.11      80600
        :param dict_data_pre:
                        前一交易日数据 格式类似本次返回的数据
        :return:
        """
        dict_data = {'security_code': Utils.quotes_surround(self.security_code)}
        # 日涨跌幅计算
        section_tail1 = section.tail(1)
        idx = section_tail1.index.values[0]
        the_date = idx.astype('M8[ms]').astype('O')
        week_day = Utils.format_week_day(the_date)
        dict_data['week_day'] = week_day
        the_date = Utils.format_date(the_date)
        dict_data['the_date'] = Utils.quotes_surround(the_date)
        if dict_data_pre is None:
            print('第一次dict_data_pre为None')
            # 转换时间格式，结果为<class 'datetime.datetime'> 1991-11-29 00:00:00
            dict_data_pre = self.get_dict_data_pre(the_date)
            if dict_data_pre is None:
                print('查询后dict_data_pre还是为None，只能创建为0的数据')
                dict_data_pre = self.create_blank_dict_data()
        self.processing_1_day_chg(section_tail1, idx, dict_data, dict_data_pre)
        for days in [1, 3, 5, 10]:
            self.processing_avg(section.tail(days), days, dict_data, dict_data_pre)
        return dict_data

    def get_dict_data_pre(self, the_date):
        sql = "select close, open, high, low, amount, vol, " \
              "price_avg_1, price_avg_1_chg, price_avg_3, price_avg_3_chg, " \
              "price_avg_5, price_avg_5_chg, price_avg_10, price_avg_10_chg " \
              "from tquant_stock_history_quotation " \
              "where security_code = {security_code} and the_date < {the_date} " \
              "order by the_date desc limit 1"
        sql = sql.format(security_code=self.security_code, the_date=Utils.quotes_surround(the_date))
        tuple_datas = self.dbService.query(sql)
        if tuple_datas is not None and len(tuple_datas) > 0:
            tuple_data = tuple_datas[0]
            if tuple_data is not None and len(tuple_data) > 0:
                if tuple_data[0] is None:
                    return None
                else:
                    dict_data = {}
                    dict_data['close'] = tuple_data[0]
                    dict_data['open'] = tuple_data[1]
                    dict_data['high'] = tuple_data[2]
                    dict_data['low'] = tuple_data[3]
                    dict_data['amount'] = tuple_data[4]
                    dict_data['vol'] = tuple_data[5]
                    dict_data['price_avg_1'] = tuple_data[6]
                    dict_data['price_avg_1_chg'] = tuple_data[7]
                    dict_data['price_avg_3'] = tuple_data[8]
                    dict_data['price_avg_3_chg'] = tuple_data[9]
                    dict_data['price_avg_5'] = tuple_data[10]
                    dict_data['price_avg_5_chg'] = tuple_data[11]
                    dict_data['price_avg_10'] = tuple_data[12]
                    dict_data['price_avg_10_chg'] = tuple_data[13]
                    return dict_data
            else:
                return None
        else:
            return None

    def create_blank_dict_data(self):
        dict_data = {}
        dict_data['close'] = 0
        dict_data['open'] = 0
        dict_data['high'] = 0
        dict_data['low'] = 0
        dict_data['amount'] = 0
        dict_data['vol'] = 0
        dict_data['price_avg_1'] = 0
        dict_data['price_avg_1_chg'] = 0
        dict_data['price_avg_3'] = 0
        dict_data['price_avg_3_chg'] = 0
        dict_data['price_avg_5'] = 0
        dict_data['price_avg_5_chg'] = 0
        dict_data['price_avg_10'] = 0
        dict_data['price_avg_10_chg'] = 0
        return dict_data

    def processing_avg(self, section_tail, days, dict_data, dict_data_pre):
        """
        section_tail.describe()结果示例 <class 'pandas.core.frame.DataFrame'>
                     amount        close  code         high          low         open            vol
        count  6.067000e+03  6067.000000  6067  6067.000000  6067.000000  6067.000000   6.067000e+03
        max    8.596942e+09    14.280000     1    14.460000    13.870000    14.460000   5.086050e+08
        :param section_tail:
        :param days:
        :param dict_data:
        :param dict_data_pre:
        :return:
        """
        try:
            # print(days, section_tail)
            section_describe = section_tail.sum()

            amount_count = section_describe['amount']
            vol_count = section_describe['vol']

            price_avg_ = "price_avg_"
            _chg = "_chg"
            _chg_diff = "_chg_diff"
            close_price_avg_ = "close_price_avg_"

            days = str(days)
            price_avg = Utils.base_round_zero(Utils.division_zero(amount_count, vol_count), 2)
            dict_data[price_avg_ + days] = price_avg
            price_avg_pre = dict_data_pre[price_avg_ + days]
            price_avg_chg = Utils.base_round_zero(Utils.division_zero(price_avg - price_avg_pre, price_avg_pre) * 100, 2)
            dict_data[price_avg_ + days + _chg] = price_avg_chg

            price_avg_chg_pre = dict_data_pre[price_avg_ + days + _chg]
            price_avg_chg_diff = price_avg_chg - price_avg_chg_pre
            dict_data[price_avg_ + days + _chg_diff] = price_avg_chg_diff

            close = dict_data['close']
            close_price_avg_chg = Utils.base_round_zero(Utils.division_zero(close - price_avg, price_avg) * 100, 2)
            dict_data[close_price_avg_ + days + _chg] = close_price_avg_chg
        except Exception:
            traceback.print_exc()

    def processing_1_day_chg(self, section_tail1, idx, dict_data, dict_data_pre):
        try:
            amount = section_tail1.at[idx, 'amount']
            # amount的类型为numpy.ndarray，是一个多维数组，可能包含多个值，其他的字段也是一样，测试的时候发现有异常抛出
            if isinstance(amount, numpy.ndarray) and amount.size > 1:
                amount = amount.tolist()[0]
            amount = Utils.base_round(amount, 2)
            amount_pre = dict_data_pre['amount']
            amount_chg = Utils.base_round_zero(Utils.division_zero(amount - amount_pre, amount_pre) * 100, 2)
            dict_data['amount'] = amount
            dict_data['amount_chg'] = amount_chg

            vol = section_tail1.at[idx, 'vol']
            if isinstance(vol, numpy.ndarray) and vol.size > 1:
                vol = vol.tolist()[0]
            vol = Utils.base_round(vol, 2)
            vol_pre = dict_data_pre['vol']
            vol_chg = Utils.base_round_zero(Utils.division_zero(vol - vol_pre, vol_pre) * 100, 2)
            dict_data['vol'] = vol
            dict_data['vol_chg'] = vol_chg

            open = section_tail1.at[idx, 'open']
            if isinstance(open, numpy.ndarray) and open.size > 1:
                open = open.tolist()[0]
            open = Utils.base_round(open, 2)
            open_pre = dict_data_pre['open']
            open_chg = Utils.base_round_zero(Utils.division_zero(open - open_pre, open_pre) * 100, 2)
            dict_data['open'] = open
            dict_data['open_chg'] = open_chg

            high = section_tail1.at[idx, 'high']
            if isinstance(high, numpy.ndarray) and high.size > 1:
                high = high.tolist()[0]
            high = Utils.base_round(high, 2)
            high_pre = dict_data_pre['high']
            high_chg = Utils.base_round_zero(Utils.division_zero(high - high_pre, high_pre) * 100, 2)
            dict_data['high'] = high
            dict_data['high_chg'] = high_chg

            low = section_tail1.at[idx, 'low']
            if isinstance(low, numpy.ndarray) and low.size > 1:
                low = low.tolist()[0]
            low = Utils.base_round(low, 2)
            low_pre = dict_data_pre['low']
            low_chg = Utils.base_round_zero(Utils.division_zero(low - low_pre, low_pre) * 100, 2)
            dict_data['low'] = low
            dict_data['low_chg'] = low_chg

            close = section_tail1.at[idx, 'close']
            if isinstance(close, numpy.ndarray) and close.size > 1:
                close = close.tolist()[0]
            close = Utils.base_round(close, 2)
            close_pre = dict_data_pre['close']
            close_chg = Utils.base_round_zero(Utils.division_zero(close - close_pre, close_pre) * 100, 2)
            dict_data['close'] = close
            dict_data['close_chg'] = close_chg

            close_open_chg = Utils.base_round_zero(Utils.division_zero(close - open, open) * 100, 2)
            dict_data['close_open_chg'] = close_open_chg
        except Exception:
            traceback.print_exc()
            # log_list = [Utils.get_now(), Utils.get_info(), self.get_classs_name(), self.security_code]
            # log_list.append(self.get_method_name())
            # log_list.append(traceback.format_exc())
            # Utils.print_log(log_list)
    #################################################################################################

    def get_max_the_date(self):
        sql = "select the_date from tquant_stock_history_quotation where security_code = {security_code} order by the_date desc limit 10"
        the_dates = self.dbService.query(sql.format(security_code=Utils.quotes_surround(self.security_code)))
        if the_dates is not None and len(the_dates) > 0:
            return the_dates[len(the_dates) - 1][0]
        else:
            return None

    def processing_day_kline(self):
        """
        股票日K数据处理，分全量还是增量
        :return: 
        """
        try:
            if self.is_reset:
                max_the_date = datetime.datetime.now().replace(year=1970, month=1, day=1)
            else:
                max_the_date = self.get_max_the_date()
            diff_days = Utils.diff_days(max_the_date)
            log_list = [Utils.get_now(), Utils.get_info(), self.get_classs_name(), self.security_code,
                        self.get_method_name(), '最大交易日', max_the_date, '距今', Utils.format_date(datetime.date.today()), '差', diff_days, '天']
            Utils.print_log(log_list)
            dict_data_pre = None
            if diff_days is None:
                result = tt.get_all_daybar(self.security_code, 'bfq')
            else:
                result = tt.get_last_n_daybar(self.security_code, diff_days, 'bfq')

            if result.empty == False:
                # 按照正序排序，时间小的排前面
                result.sort_index(ascending=True)
                # 需要处理的单只股票进度计数
                add_up = 0
                # 需要处理的单只股票进度打印字符
                process_line = '='
                list_db_data = []
                len_indexes = len(result.index.values)
                for i in range(len_indexes):
                    add_up += 1
                    if i < 9:
                        continue
                    end = i + 1
                    start = i - 9
                    section = result.iloc[start:end, :]
                    # print('#########################', add_up)
                    # print(section)
                    dict_data = self.processing_section(section, dict_data_pre)
                    # print(i, 'dict_data', dict_data)
                    list_db_data.append(dict_data)
                    dict_data_pre = dict_data
                    # print(dict_data)
                    self.dbService.upsert(dict_data, 'tquant_stock_history_quotation', ['security_code', 'the_date'])
                    # 批量打印日志
                    if len(list_db_data) % 200 == 0:
                        process_line += '='
                        progress = Utils.base_round(Utils.division_zero(add_up, len_indexes) * 100, 2)
                        log_list = [Utils.get_now(), Utils.get_info(), self.get_classs_name(), self.security_code,
                                    self.get_method_name(), '处理进度', add_up, len_indexes, process_line, str(progress) + '%']
                        Utils.print_log(log_list)
                process_line += '='
                progress = Utils.base_round(Utils.division_zero(add_up, len_indexes) * 100, 2)
                log_list = [Utils.get_now(), Utils.get_info(), self.get_classs_name(), self.security_code,
                            self.get_method_name(), '处理进度', add_up, len_indexes, process_line, str(progress) + '%']
                Utils.print_log(log_list)
                # self.dbService.upsert_many(list_db_data, 'tquant_stock_history_quotation', ['security_code', 'the_date'])
            else:
                log_list = [Utils.get_now(), Utils.get_warn(), self.get_classs_name(), self.security_code,
                            self.get_method_name(), '【日K DataFrame为空】']
                Utils.print_log(log_list)
        except Exception:
            traceback.print_exc()
            # log_list = [Utils.get_now(), Utils.get_info(), self.get_classs_name(), self.security_code,
            #             self.get_method_name(), traceback.format_exc()]
            # Utils.print_log(log_list)
    #############################################################################################################

    def processing_real_time_kline(self):
        """
        处理单只股票的实时行情
        :return: 
        """
        try:
            current_date = datetime.datetime.now().replace(microsecond=0)
            start_date = current_date.replace(hour=9, minute=30, second=0, microsecond=0)
            end_date = current_date.replace(hour=15, minute=0, second=0, microsecond=0)
            if current_date <= end_date and current_date >= start_date:
                log_list = [Utils.get_now(), Utils.get_info(), self.get_classs_name(), self.security_code,
                            self.get_method_name(), '当日全部5分钟实时行情 开始时间', start_date, '拉取时间', current_date,
                            '结束时间', end_date]
                Utils.print_log(log_list)
                # if True:
                # 5分钟K的实时行情
                day_kline = tt.get_stock_bar(self.security_code, 1)
                # print('day_kline', day_kline)
                # 处理单只股票的实时行情，并入库
                self.analysis_real_time_kline(day_kline, start_date)
                # 股票日K涨跌幅处理方法
                self.procesing_day_kline_after()
            else:
                log_list = [Utils.get_now(), Utils.get_warn(), self.get_classs_name(), self.security_code,
                            self.get_method_name(), '当日全部5分钟实时行情 开始时间', start_date, '拉取时间', current_date,
                            '结束时间', end_date, '【交易结束或不在交易时间段内】']
                Utils.print_log(log_list)
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