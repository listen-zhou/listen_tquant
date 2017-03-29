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
    def __init__(self, dbService, logger, sleep_seconds, one_time):
        super(CalendarService, self).__init__(logger)
        self.base_log_list = self.deepcopy_list().append(self.get_clsss_name())
        self.base_info(self.deepcopy_from_list(self.base_log_list).append(self.get_method_name()))
        self.dbService = dbService
        self.sleep_seconds = sleep_seconds
        self.one_time = one_time
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
            if self.one_time:
                break
            time.sleep(self.sleep_seconds)

    def processing(self):
        """
        调用交易日查询接口，返回信息，并解析入库
        :return:
        """
        log_list = self.deepcopy_from_list(self.base_log_list)
        log_list.append(self.get_method_name())

        start_log_list = self.deepcopy_from_list(log_list)
        start_log_list.append('【start】')
        self.base_info(start_log_list)
        upsert_sql_list = []
        add_up = 0
        process_line = ''
        try:
            # 全部交易日数据
            result = tt.get_calendar('1970-01-01', '2018-01-01')
            len_result = len(result)

            calendar_log_list = self.deepcopy_from_list(log_list)
            calendar_log_list.append('tt.get_calendar() result size ')
            calendar_log_list.append(len_result)
            self.base_info(calendar_log_list)

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
                    if len(upsert_sql_list) == 100:
                        self.dbService.insert_many(upsert_sql_list)
                        upsert_sql_list = []
                        process_line += '='
                        processing = self.base_round(Decimal(add_up) / Decimal(len_result), 4) * 100
                        upsert_sql_list.append(upsert_sql)

                        batch_log_list = self.deepcopy_from_list(log_list)
                        batch_log_list.append('inner')
                        batch_log_list.append(len_result)
                        batch_log_list.append(process_line)
                        batch_log_list.append(str(processing) + '%')
                        self.base_info(batch_log_list)

                    else:
                        upsert_sql_list.append(upsert_sql)
                except Exception:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    except_log_list = self.deepcopy_from_list(log_list)
                    except_log_list.append(exc_type)
                    except_log_list.append(exc_value)
                    except_log_list.apend(exc_traceback)
                    self.base_error(except_log_list)
            if len(upsert_sql_list) > 0:
                self.dbService.insert_many(upsert_sql_list)
                process_line += '='
            processing = self.base_round(Decimal(add_up) / Decimal(len_result), 4) * 100

            batch_log_list = self.deepcopy_from_list(log_list)
            batch_log_list.append('outer')
            batch_log_list.append(len_result)
            batch_log_list.append(process_line)
            batch_log_list.append(str(processing) + '%')
            self.base_info(batch_log_list)

        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            except_log_list = self.deepcopy_from_list(log_list)
            except_log_list.append(exc_type)
            except_log_list.append(exc_value)
            except_log_list.apend(exc_traceback)
            self.base_error(except_log_list)
        end_log_list = self.deepcopy_from_list(log_list)
        end_log_list.append('【end】')
        self.base_info(end_log_list)