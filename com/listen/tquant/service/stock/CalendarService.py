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
        self.dbService = dbService
        self.sleep_seconds = sleep_seconds
        self.one_time = one_time

        self.log_list = [self.get_clsss_name()]

        init_log_list = self.deepcopy_list(self.log_list)
        init_log_list.append(self.get_method_name())
        init_log_list.append('sleep seconds')
        init_log_list.append(sleep_seconds)
        init_log_list.append('one_time')
        init_log_list.append(one_time)
        self.logger.info(init_log_list)

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
        调用交易日查询接口，返回信息，并解析入库
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

        upsert_sql_list = []
        add_up = 0
        process_line = ''
        try:
            # 全部交易日数据
            result = tt.get_calendar('1970-01-01', '2018-01-01')
            len_result = len(result)

            calendar_log_list = self.deepcopy_list(processing_log_list)
            calendar_log_list.append('tt.get_calendar() result size ')
            calendar_log_list.append(len_result)
            self.logger.info(calendar_log_list)

            for calendar in result:
                add_up += 1
                try:
                    # 交易日
                    the_date = calendar.date()
                    # 是否月末，1-是，0-否
                    is_month_end = 1 if calendar.is_month_end else 0
                    # 是否月初，1-是，0-否
                    is_month_start = 1 if calendar.is_month_start else 0
                    # 是否季末，1-是，0-否
                    is_quarter_end = 1 if calendar.is_quarter_end else 0
                    # 是否季末，1-是，0-否
                    is_quarter_start = 1 if calendar.is_quarter_start else 0
                    # 是否年末，1-是，0-否
                    is_year_end = 1 if calendar.is_year_end else 0
                    # 是否初末，1-是，0-否
                    is_year_start = 1 if calendar.is_year_start else 0
                    # 周几，从0开始，即0-周一，1-周二
                    day_of_week = calendar.dayofweek
                    # 当前所在周是年当中的第几周
                    week_of_year = calendar.weekofyear
                    # 季度，从1开始
                    quarter = calendar.quarter
                    # 年度
                    year = calendar.year
                    # 月度
                    month = calendar.month

                    upsert_sql = self.upsert_calendar_info_sql.format(
                        the_date=self.quotes_surround(str(the_date)),
                        is_month_end=is_month_end,
                        is_month_start=is_month_start,
                        is_quarter_end=is_quarter_end,
                        is_quarter_start=is_quarter_start,
                        is_year_end=is_year_end,
                        is_year_start=is_year_start,
                        day_of_week=day_of_week,
                        week_of_year=week_of_year,
                        quarter=quarter,
                        year=year,
                        month=month
                    )
                    if len(upsert_sql_list) == 1000:
                        self.dbService.insert_many(upsert_sql_list)
                        upsert_sql_list = []
                        process_line += '='
                        processing = self.base_round(Decimal(add_up) / Decimal(len_result) * 100, 2)
                        upsert_sql_list.append(upsert_sql)

                        batch_log_list = self.deepcopy_list(processing_log_list)
                        batch_log_list.append('inner')
                        batch_log_list.append(add_up)
                        batch_log_list.append(len_result)
                        batch_log_list.append(process_line)
                        batch_log_list.append(str(processing) + '%')
                        self.logger.info(batch_log_list)

                    else:
                        upsert_sql_list.append(upsert_sql)
                except Exception:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    except_log_list = self.deepcopy_list(processing_log_list)
                    except_log_list.append(exc_type)
                    except_log_list.append(exc_value)
                    except_log_list.append(exc_traceback)
                    self.logger.error(except_log_list)
            if len(upsert_sql_list) > 0:
                self.dbService.insert_many(upsert_sql_list)
                process_line += '='
            processing = self.base_round(Decimal(add_up) / Decimal(len_result) * 100, 2)

            batch_log_list = self.deepcopy_list(processing_log_list)
            batch_log_list.append('outer')
            batch_log_list.append(add_up)
            batch_log_list.append(len_result)
            batch_log_list.append(process_line)
            batch_log_list.append(str(processing) + '%')
            self.logger.info(batch_log_list)

        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            except_log_list = self.deepcopy_list(processing_log_list)
            except_log_list.append(exc_type)
            except_log_list.append(exc_value)
            except_log_list.append(exc_traceback)
            self.logger.error(except_log_list)

        end_log_list = self.deepcopy_list(processing_log_list)
        end_log_list.append('【end】')
        self.logger.info(end_log_list)