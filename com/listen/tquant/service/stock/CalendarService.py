# coding: utf-8


from decimal import *
import decimal
context = decimal.getcontext()
context.rounding = decimal.ROUND_05UP

import tquant as tt
import time
import sys

from com.listen.tquant.service.BaseService import BaseService


class CalendarService(BaseService):
    """
    交易日信息处理服务
    """
    def __init__(self, dbService, logger, sleep_seconds):
        super(CalendarService, self).__init__(logger)
        self.base_info('{0[0]} ...', [self.get_current_method_name()])
        self.dbService = dbService
        self.sleep_seconds = sleep_seconds
        self.upsert_calendar_info_sql = "insert into tquant_calendar_info (the_date, is_month_end, is_month_start, " \
                                        "is_quarter_end, is_quarter_start, is_year_end, is_year_start, day_of_week, " \
                                        "week_of_year, quarter, year, month) " \
                                        "values ({the_date}, {is_month_end}, {is_month_start}, {is_quarter_end}, " \
                                        "{is_quarter_start}, {is_year_end}, {is_year_start}, {day_of_week}, " \
                                        "{week_of_year}, {quarter}, {year}, {month})" \
                                        "on duplicate key update " \
                                        "is_month_end=values(is_month_end), is_month_start=values(is_month_start), is_quarter_end=values(is_quarter_end), " \
                                        "is_quarter_start=values(is_quarter_start), is_year_end=values(is_year_end), is_year_start=values(is_year_start), " \
                                        "day_of_week=values(day_of_week), week_of_year=values(week_of_year), quarter=values(quarter), " \
                                        "year=values(year), month=values(month)"

    def loop(self):
        while True:
            self.processing()
            time.sleep(self.sleep_seconds)

    def processing(self):
        """
        调用交易日查询接口，返回信息，并解析入库
        :return:
        """
        self.base_info('{0[0]} 【start】...', [self.get_current_method_name()])
        upsert_sql_list = []
        add_up = 0
        process_line = ''
        try:
            # 全部交易日数据
            result = tt.get_calendar('1970-01-01', '2018-01-01')
            len_result = len(result)
            for calendar in result:
                add_up += 1
                # 定义临时存储单行数据的字典，用以后续做执行sql的数据填充
                value_dict = {}
                try:
                    # 交易日
                    value_dict['the_date'] = str(calendar.date())
                    # 是否月末，1-是，0-否
                    value_dict['is_month_end'] = 1 if calendar.is_month_end else 0
                    # 是否月初，1-是，0-否
                    value_dict['is_month_start'] = 1 if calendar.is_month_start else 0
                    # 是否季末，1-是，0-否
                    value_dict['is_quarter_end'] = 1 if calendar.is_quarter_end else 0
                    # 是否季末，1-是，0-否
                    value_dict['is_quarter_start'] = 1 if calendar.is_quarter_start else 0
                    # 是否年末，1-是，0-否
                    value_dict['is_year_end'] = 1 if calendar.is_year_end else 0
                    # 是否初末，1-是，0-否
                    value_dict['is_year_start'] = 1 if calendar.is_year_start else 0
                    # 周几，从0开始，即0-周一，1-周二
                    value_dict['day_of_week'] = calendar.dayofweek
                    # 当前所在周是年当中的第几周
                    value_dict['week_of_year'] = calendar.weekofyear
                    # 第几季度，从1开始
                    value_dict['quarter'] = calendar.quarter
                    # 年度
                    value_dict['year'] = calendar.year
                    # 月度
                    value_dict['month'] = calendar.month

                    upsert_sql = self.upsert_calendar_info_sql.format(
                        the_date=self.quotes_surround(value_dict['the_date']),
                        is_month_end=value_dict['is_month_end'],
                        is_month_start=value_dict['is_month_start'],
                        is_quarter_end=value_dict['is_quarter_end'],
                        is_quarter_start=value_dict['is_quarter_start'],
                        is_year_end=value_dict['is_year_end'],
                        is_year_start=value_dict['is_year_start'],
                        day_of_week=value_dict['day_of_week'],
                        week_of_year=value_dict['week_of_year'],
                        quarter=value_dict['quarter'],
                        year=value_dict['year'],
                        month=value_dict['month']
                    )
                    if len(upsert_sql_list) == 1000:
                        self.dbService.insert_many(upsert_sql_list)
                        upsert_sql_list = []
                        process_line += '='
                        processing = self.base_round(Decimal(add_up) / Decimal(len_result), 4) * 100
                        upsert_sql_list.append(upsert_sql)
                        self.base_info('{0[0]} {0[1]} processing {0[2]} {0[3]} {0[4]}%...',
                                       [self.get_current_method_name(), 'inner', len_result, process_line, processing])
                        # time.sleep(1)
                    else:
                        upsert_sql_list.append(upsert_sql)
                except Exception:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    self.base_error('{0[0]} {0[1]} {0[2]} {0[3]} ',
                                    [self.get_current_method_name(), exc_type, exc_value, exc_traceback])
            if len(upsert_sql_list) > 0:
                self.dbService.insert_many(upsert_sql_list)
                process_line += '='
            processing = self.base_round(Decimal(add_up) / Decimal(len_result), 4) * 100
            self.base_info('{0[0]} {0[1]} {0[2]} {0[3]} {0[4]}%',
                           [self.get_current_method_name(), 'outer', len_result, process_line, processing])

        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.base_error('{0[0]} {0[1]} {0[2]} {0[3]} ',
                            [self.get_current_method_name(), exc_type, exc_value, exc_traceback])
        self.base_info('{0[0]} 【end】', [self.get_current_method_name()])