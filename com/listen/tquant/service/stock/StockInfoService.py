# coding: utf-8
from decimal import *
import decimal
context = decimal.getcontext()
context.rounding = decimal.ROUND_05UP

import tquant as tt
import time
import sys

from com.listen.tquant.service.BaseService import BaseService


class StockInfoService(BaseService):
    """
    股票基本信息处理服务
    """
    def __init__(self, dbService, logger, sleep_seconds, one_time):
        super(StockInfoService, self).__init__(logger)
        self.dbService = dbService
        self.sleep_seconds = sleep_seconds
        self.one_time = one_time
        self.security_type = 'STOCK'
        self.log_list = [self.get_clsss_name()]

        init_log_list = self.deepcopy_list(self.log_list)
        init_log_list.append(self.get_method_name())
        init_log_list.append('sleep seconds')
        init_log_list.append(sleep_seconds)
        init_log_list.append('one_time')
        init_log_list.append(one_time)
        self.logger.info(init_log_list)

        self.upsert_stock_info_sql = "insert into tquant_security_info (security_code, security_name, " \
                                    "security_type, exchange_code) " \
                                    "values ({security_code}, {security_name}, {security_type}, {exchange_code}) " \
                                    "on duplicate key update " \
                                    "security_name=values(security_name) "

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
        调用股票查询接口，返回全部A股基本信息，并解析入库
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

        try:
            # 股票基本信息返回结果为 DataFrame
            stock_list = tt.get_stocklist()
            if stock_list is not None and stock_list.empty is False:
                # 索引对象的值为list
                indexes_values = stock_list.index.values
                # 每行的列头中包含两个字段id, name，分别对应股票代码和股票简称，股票代码中包含有交易所标识信息
                columns_values = ['id', 'name']
                # 临时存储需要批量执行更新的sql语句，是个list
                upsert_sql_list = []
                # 数据处理进度计数
                add_up = 0
                # 数据处理进度打印字符
                process_line = '='
                len_indexes_values = len(indexes_values)
                for idx in indexes_values:
                    add_up += 1
                    try:
                        security_code = idx
                        security_name = stock_list.at[idx, 'name']
                        exchange_code = stock_list.at[idx, 'id']
                        exchange_code = exchange_code[0:exchange_code.find(idx)].upper()
                        upsert_sql = self.upsert_stock_info_sql.format(
                            exchange_code=self.quotes_surround(exchange_code),
                            security_code=self.quotes_surround(security_code),
                            security_name=self.quotes_surround(security_name),
                            security_type=self.quotes_surround(self.security_type)
                        )
                        if len(upsert_sql_list) == 100:
                            self.dbService.insert_many(upsert_sql_list)
                            upsert_sql_list = []
                            process_line += '='
                            processing = self.base_round(Decimal(add_up) / Decimal(len_indexes_values) * 100, 2)
                            upsert_sql_list.append(upsert_sql)
                            batch_log_list = self.deepcopy_list(processing_log_list)
                            batch_log_list.append('inner')
                            batch_log_list.append(add_up)
                            batch_log_list.append(len_indexes_values)
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
                processing = self.base_round(Decimal(add_up) / Decimal(len_indexes_values) * 100, 2)
                batch_log_list = self.deepcopy_list(processing_log_list)
                batch_log_list.append('outer')
                batch_log_list.append(add_up)
                batch_log_list.append(len_indexes_values)
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