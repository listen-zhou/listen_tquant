# coding: utf-8

import tquant as tt
import threading
from com.listen.tquant.dbservice.Service import DbService


class StockInfoThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.dbService = DbService()
        self.upsert_stockinfo_sql = "insert into tquant_security_info (security_code, security_name, " \
                                  "security_type, exchange_code) " \
                                  "values ({security_code},{security_name},{security_type},{exchange_code}) " \
                                  "on duplicate key update " \
                                  "security_name=values(security_name) "

    def run(self):
        print(self.name, 'starting ...')
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
            upsert_sql = self.upsertStockInfoSql.format(exchange_code="'"+value_dict['exchange_code']+"'",
                                                        security_code="'"+value_dict['security_code']+"'",
                                                        security_name="'"+value_dict['security_name']+"'",
                                                        security_type="'"+value_dict['security_type']+"'")
            upsert_sql_list.append(upsert_sql)
        result = self.dbService.insert_many(upsert_sql_list)
        print(upsert_sql_list, result)


