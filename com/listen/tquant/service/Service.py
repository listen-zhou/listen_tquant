# coding: utf-8
import datetime

import tquant as tt
import threading
from com.listen.tquant.dbservice.Service import DbService


class StockInfoService():
    def __init__(self, dbService):
        print('starting ... {}'.format(datetime.datetime.now()))
        self.dbService = dbService


    def get_stock_info(self):
        print('run ... {}'.format(datetime.datetime.now()))
        upsert_stock_info_sql = "insert into tquant_security_info (security_code, security_name, " \
                                    "security_type, exchange_code) " \
                                    "values ({security_code},{security_name},{security_type},{exchange_code}) " \
                                    "on duplicate key update " \
                                    "security_name=values(security_name) "
        # stock_list type is DataFrame
        stock_list = tt.get_stocklist()
        # indexes_values type is []
        indexes_values = stock_list.index.values
        # columns_values type is [], it comes from stock_list.columns.values a few field
        columns_values = ['id', 'name']
        # upsert_sql_list is store upsert_sql
        upsert_sql_list = []
        for idx in indexes_values:
            # value is dict, it temp store database data
            value_dict = {}
            value_dict['security_type'] = "STOCK"
            value_dict['security_code'] = idx
            for column in columns_values:
                if column == 'name':
                    value_dict['security_name'] = stock_list.at[idx, column]
                else:
                    value_dict['exchange_code'] = stock_list.at[idx, column][0:stock_list.at[idx, column].find(idx)].upper()
            upsert_sql = upsert_stock_info_sql.format(exchange_code="'"+value_dict['exchange_code']+"'",
                                                        security_code="'"+value_dict['security_code']+"'",
                                                        security_name="'"+value_dict['security_name']+"'",
                                                        security_type="'"+value_dict['security_type']+"'")
            upsert_sql_list.append(upsert_sql)
        result = self.dbService.insert_many(upsert_sql_list)
        print(len(upsert_sql_list), result)

    def get_all_stock_day_kline(self):
        query_stock_code_sql = 'select security_code from tquant_security_info where security_type = "STOCK" '
        upsert_stock_day_kline_sql = 'insert into tquant_stock_day_kline (security_code, the_date, ' \
                     'amount, vol, open, high, low, close) ' \
                     'values ({security_code}, {the_date}, ' \
                     '{amount}, {vol}, {open}, {high}, {low}, {close}) ' \
                     'on duplicate key update ' \
                     'amount=values(amount), vol=values(vol), open=values(open), ' \
                     'high=values(high), low=values(low), close=values(close) '
        stock_tuple_tuple = self.dbService.query(query_stock_code_sql)
        if stock_tuple_tuple:
            for stock_item in stock_tuple_tuple:
                security_code = stock_item[0]
                print(security_code)
                day_kline = tt.get_all_daybar(security_code, 'bfq')
                indexes_values = day_kline.index.values
                columns_values = day_kline.columns.values
                upsert_sql_list = []
                for idx in indexes_values:
                    the_date = (str(idx))[0:10]
                    value_dict = {}
                    value_dict['security_code'] = security_code
                    value_dict['the_date'] = the_date
                    for column in columns_values:
                        if column == 'amount':
                            value_dict['amount'] = round(float(day_kline.at[idx, column])/10000, 4)
                        elif column == 'vol':
                            value_dict['vol'] = round(float(day_kline.at[idx, column])/1000000, 4)
                        elif column == 'open':
                            value_dict['open'] = round(float(day_kline.at[idx, column]), 2)
                        elif column == 'high':
                            value_dict['high'] = round(float(day_kline.at[idx, column]), 2)
                        elif column == 'low':
                            value_dict['low'] = round(float(day_kline.at[idx, column]), 2)
                        elif column == 'close':
                            value_dict['close'] = round(float(day_kline.at[idx, column]), 2)
                    print(value_dict)
                    upsert_sql = upsert_stock_day_kline_sql.format(security_code="'"+value_dict['security_code']+"'",
                                                                   the_date="'"+value_dict['the_date']+"'",
                                                                   amount=value_dict['amount'],
                                                                   vol=value_dict['vol'],
                                                                   open=value_dict['open'],
                                                                   high=value_dict['high'],
                                                                   low=value_dict['low'],
                                                                   close=value_dict['close'])
                    if len(upsert_sql_list) == 100:
                        self.dbService.insert_many(upsert_sql_list)
                        upsert_sql_list = []
                        upsert_sql_list.append(upsert_sql)
                    else:
                        upsert_sql_list.append(upsert_sql)
                if len(upsert_sql_list) > 0:
                    self.dbService.insert_many(upsert_sql_list)




