# coding: utf-8
import traceback
from decimal import *
import types

import numpy
import tquant as tt
import datetime
import time


class CalendarService():
    """
    交易日信息处理服务
    调用tquant交易日接口，处理返回数据，并入库
    """
    def __init__(self, dbService):
        print(datetime.datetime.now(), 'CalendarServiceService init ... {}'.format(datetime.datetime.now()))
        self.dbService = dbService
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

    def get_calendar_info(self):
        """
        调用交易日查询接口，返回信息，并解析入库
        :return:
        """
        print(datetime.datetime.now(), 'CalendarServiceService get_calendar_info start ... {}'.format(datetime.datetime.now()))
        upsert_sql_list = []
        add_up = 0
        process_line = ''
        try:
            # 全部交易日数据
            result_list = tt.get_calendar('1970-01-01', '2018-01-01')
            for calendar in result_list:
                # print(dir(calendar))
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
                        the_date="'" + value_dict['the_date'] + "'",
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
                    if len(upsert_sql_list) == 100:
                        self.dbService.insert_many(upsert_sql_list)
                        upsert_sql_list = []
                        process_line += '='
                        processing = Decimal(add_up) / Decimal(len(result_list)) * 100
                        upsert_sql_list.append(upsert_sql)
                        print(datetime.datetime.now(), 'CalendarServiceService inner get_calendar_info size:', len(result_list), 'processing ', process_line,
                              str(processing) + '%')
                        add_up += 1
                        # time.sleep(1)
                    else:
                        upsert_sql_list.append(upsert_sql)
                        add_up += 1
                except Exception:
                    traceback.print_exc()
            if len(upsert_sql_list) > 0:
                self.dbService.insert_many(upsert_sql_list)
                process_line += '='
            processing = Decimal(add_up) / Decimal(len(result_list)) * 100
            print(datetime.datetime.now(), 'CalendarServiceService outer get_calendar_info size:', len(result_list), 'processing ', process_line, str(processing) + '%')
            print(datetime.datetime.now(), 'CalendarServiceService =============================================')
        except Exception:
            traceback.print_exc()
        print(datetime.datetime.now(), 'CalendarServiceService get_calendar_info end ... {}'.format(datetime.datetime.now()))