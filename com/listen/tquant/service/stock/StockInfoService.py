# coding: utf-8
import traceback
from decimal import *
import types

import numpy
import tquant as tt
import datetime
import time


class StockInfoService():
    """
    股票基本信息处理服务
    调用tquant全部A股接口，处理返回数据，并入库
    """
    def __init__(self, dbService):
        print(datetime.datetime.now(), 'StockInfoService init ... {}'.format(datetime.datetime.now()))
        self.dbService = dbService
        self.upsert_stock_info_sql = "insert into tquant_security_info (security_code, security_name, " \
                                    "security_type, exchange_code) " \
                                    "values ({security_code},{security_name},{security_type},{exchange_code}) " \
                                    "on duplicate key update " \
                                    "security_name=values(security_name) "

    def get_stock_info(self):
        """
        调用股票查询接口，返回全部A股基本信息，并解析入库
        :return:
        """
        print(datetime.datetime.now(), 'StockInfoService get_stock_info start ... {}'.format(datetime.datetime.now()))
        getcontext().prec = 4
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
                        processing = Decimal(add_up) / Decimal(len(indexes_values)) * 100
                        upsert_sql_list.append(upsert_sql)
                        print(datetime.datetime.now(), 'StockInfoService inner get_stock_info size:', len(indexes_values), 'processing ', process_line,
                              str(processing) + '%')
                        add_up += 1
                        time.sleep(1)
                    else:
                        upsert_sql_list.append(upsert_sql)
                        add_up += 1
                except Exception:
                    print(datetime.datetime.now(), 'StockInfoService except exception value_dict:', value_dict)
                    traceback.print_exc()
            if len(upsert_sql_list) > 0:
                self.dbService.insert_many(upsert_sql_list)
                process_line += '='
            processing = Decimal(add_up) / Decimal(len(indexes_values)) * 100
            print(datetime.datetime.now(), 'StockInfoService outer get_stock_info size:', len(indexes_values), 'processing ', process_line,
                  str(processing) + '%')
            print(datetime.datetime.now(), 'StockInfoService =============================================')
        except Exception:
            traceback.print_exc()
        print(datetime.datetime.now(), 'StockInfoService get_stock_info end ... {}'.format(datetime.datetime.now()))