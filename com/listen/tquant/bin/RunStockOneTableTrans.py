# coding: utf-8

import multiprocessing
import time
from com.listen.tquant.dbservice.Service import DbService
from com.listen.tquant.service.stock.StockOneTable import StockOneTable


if __name__ == '__main__':
    service = StockOneTable('', False)
    service.data_transport(0, 1000)