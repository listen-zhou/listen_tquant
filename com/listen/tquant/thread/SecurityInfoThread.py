# coding: utf-8

import tquant as tt
import threading
from com.listen.tquant.dbservice.Service import DbService
from com.listen.tquant.dbservice.items.item import StockInfo
import json

class StockInfoThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.dbService = DbService()
        self.upsertStockInfoSql = "insert into tquant_security_info (security_code, security_name, " \
                                  "security_type, exchange_code) " \
                                  "values ({security_code}, {security_name}, " \
                                  "{security_type}, {exchange_code}) " \
                                  "on duplicate key update " \
                                  "security_name=values(security_name) ";

    def run(self):
        print(self.name, 'starting ...')
        stockList = tt.get_stocklist()
        indexValues = stockList.index.values
        columnsValues = ['id', 'name']
        for idx in indexValues:
            stockInfo = StockInfo(security_code=idx)
            for column in columnsValues:
                if column == 'name':
                    stockInfo.security_name = stockList.at[idx, column]
                else:
                    stockInfo.exchange_code = stockList.at[idx, column][0:stockList.at[idx, column].find(idx)].upper()
            print(stockInfo.security_name, stockInfo.security_code, stockInfo.exchange_code)


