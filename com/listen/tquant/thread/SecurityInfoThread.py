# coding: utf-8

import tquant as tt
import threading
from com.listen.tquant.dbservice.Service import DbService

class StockInfoThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.dbService = DbService()
        self.upsertStockInfoSql = "insert into tquant_security_info (security_code, security_name, " \
                                  "security_type, exchange_code) " \
                                  "values ({security_code},{security_name},{security_type},{exchange_code}) " \
                                  "on duplicate key update " \
                                  "security_name=values(security_name) ";

    def run(self):
        print(self.name, 'starting ...')
        stockList = tt.get_stocklist()
        indexValues = stockList.index.values
        columnsValues = ['id', 'name']
        upsert_sql_list = []
        for idx in indexValues:
            value = {'security_type' : 'STOCK'}
            value['security_code'] = idx
            for column in columnsValues:
                if column == 'name':
                    value['security_name'] = stockList.at[idx, column]
                else:
                    value['exchange_code'] = stockList.at[idx, column][0:stockList.at[idx, column].find(idx)].upper()
            upsert_sql = self.upsertStockInfoSql.format(exchange_code="'"+value['exchange_code']+"'",
                                                        security_code="'"+value['security_code']+"'",
                                                        security_name="'"+value['security_name']+"'",
                                                        security_type="'"+value['security_type']+"'")
            #result = self.dbService.insert(upsert_sql)
            upsert_sql_list.append(upsert_sql)
        result = self.dbService.insert_many(upsert_sql_list)
        print(upsert_sql_list, result)


