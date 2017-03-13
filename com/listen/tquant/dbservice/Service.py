# -*- coding: utf-8 -*-

import pymysql
import configparser
import json
import os

class DbService():
    def __init__(self):
        # charset必须设置为utf8，而不能为utf-8
        config = configparser.ConfigParser()
        os.chdir('../config')
        filePath = os.getcwd() + '\config.ini'
        print(filePath)
        config.read(filePath)
        mysqlSection = config['mysql']
        if mysqlSection:
            host = mysqlSection['db.host']
            port = int(mysqlSection['db.port'])
            username = mysqlSection['db.username']
            password = mysqlSection['db.password']
            dbname = mysqlSection['db.dbname']
            charset = mysqlSection['db.charset']
            self.conn = pymysql.connect(host=host, port=port, user=username, passwd=password, db=dbname, charset=charset)
            self.conn.autocommit(True)
            self.cursor = self.conn.cursor()
        else:
            raise FileNotFoundError('config.ini mysql section not found!!!')

    #数据库连接关闭
    def close(self):
        if self.cursor:
            self.cursor.close()
            print('---> 关闭游标')
        if self.conn:
            self.conn.close()
            print('---> 关闭连接')

    #通用插入方法
    def insert_item(self, item, upsert_sql):
        """插入操作-通用方法，插入单个item通用方法，item字段名须与upsert_sql字段名一致，且数据库中有唯一(组合)索引字段"""
        if (item & upsert_sql):
            item_json = json.dumps(item)
            print("format sql before:", upsert_sql)
            print('item_json:', item_json)
            upsert_sql.format(item_json)
            print("format sql after:", upsert_sql)
            self.cursor.execute(upsert_sql)

    # 没有加异常处理，请自行添加
    # on duplicate key update 这个写法是以表中的唯一索引unique字段为主，去更新其他的字段，兼顾insert和update功能，即没有唯一索引对应的数据，就insert，有就update
    def insertItem(self, item):
        if item:
            sql = "insert into spider_stock_base_info " \
                  "(comp_code, comp_url, comp_name, security_code, exchange, " \
                  "security_name, the_date, whole_capital, circulating_capital, " \
                  "announcement_url) " \
                  "values ('{0}', '{1}', '{2}', '{3}', '{4}', " \
                  "'{5}', '{6}', {7}, {8}, " \
                  "'{9}') " \
                  "on duplicate key update " \
                  "comp_code=values(comp_code), comp_url=values(comp_url), comp_name=values(comp_name), " \
                  "security_name=values(security_name), the_date=values(the_date), whole_capital=values(whole_capital), circulating_capital=values(circulating_capital), " \
                  "announcement_url=values(announcement_url) "
            sql = sql.format(item['compCode'], item['compUrl'], item['compName'], item['securityCode'], item['exchange'],
                             item['securityName'], item['theDate'], item['wholeCapital'], item['circulatingCapital']
                             , item['announcementUrl'])

            self.cursor.execute(sql)

            # print '---> insert ', item['securityCode'], item['exchange'], item['securityName'], 'success'
        else:
            print('---> 插入数据为空，插入失败！！！')

