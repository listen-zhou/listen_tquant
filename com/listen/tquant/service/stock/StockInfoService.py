# coding: utf-8
import traceback
from decimal import *
import types

import numpy
import tquant as tt
import datetime
import time
import sys

from com.listen.tquant.service.BaseService import BaseService


class StockInfoService(BaseService):
    """
    股票基本信息处理服务
    调用tquant全部A股接口，处理返回数据，并入库
    """
    def __init__(self, dbService, logger):
        super(StockInfoService, self).__init__(logger)
        self.dbService = dbService
        self.upsert_stock_info_sql = "insert into tquant_security_info (security_code, security_name, " \
                                    "security_type, exchange_code) " \
                                    "values ({security_code},{security_name},{security_type},{exchange_code}) " \
                                    "on duplicate key update " \
                                    "security_name=values(security_name) "

    def processing(self):
        """
        调用股票查询接口，返回全部A股基本信息，并解析入库
        :return:
        """
        self.base_info('{0[0]} 【start】...', [self.get_current_method_name()])
        try:
            # 股票基本信息返回结果为 DataFrame
            stock_list = tt.get_stocklist()
            # 索引对象的值为list
            indexes_values = stock_list.index.values
            # 每行的列头中包含两个字段id, name，分别对应股票代码和股票简称，股票代码中包含有交易所标识信息
            columns_values = ['id', 'name']
            # 临时存储需要批量执行更新的sql语句，是个list
            upsert_sql_list = []
            # 数据处理进度计数
            add_up = 0
            # 数据处理进度打印字符
            process_line = ''
            for idx in indexes_values:
                add_up += 1
                # 定义临时存储单行数据的字典，用以后续做执行sql的数据填充
                value_dict = {}
                try:
                    value_dict['security_type'] = "STOCK"
                    value_dict['security_code'] = idx
                    value_dict['security_name'] = stock_list.at[idx, 'name']
                    exchange_code = stock_list.at[idx, 'id']
                    exchange_code = exchange_code[0:exchange_code.find(idx)].upper()
                    value_dict['exchange_code'] = exchange_code
                    upsert_sql = self.upsert_stock_info_sql.format(
                        exchange_code="'"+value_dict['exchange_code']+"'",
                        security_code="'"+value_dict['security_code']+"'",
                        security_name="'"+value_dict['security_name']+"'",
                        security_type="'"+value_dict['security_type']+"'"
                    )
                    if len(upsert_sql_list) == 100:
                        self.dbService.insert_many(upsert_sql_list)
                        upsert_sql_list = []
                        process_line += '='
                        processing = round(Decimal(add_up) / Decimal(len(indexes_values)), 4) * 100
                        upsert_sql_list.append(upsert_sql)
                        self.base_info('{0[0]} {0[1]} {0[2]} {0[3]} {0[4]}%...',
                                       [self.get_current_method_name(), 'inner', len(indexes_values), process_line,
                                        processing])
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
            processing = round(Decimal(add_up) / Decimal(len(indexes_values)), 4) * 100
            self.base_info('{0[0]} {0[1]} {0[2]} {0[3]} {0[4]}%',
                           [self.get_current_method_name(), 'outer', len(indexes_values), process_line,
                            processing])

        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.base_error('{0[0]} {0[1]} {0[2]} {0[3]} ',
                            [self.get_current_method_name(), exc_type, exc_value, exc_traceback])
        self.base_info('{0[0]} 【end】', [self.get_current_method_name()])