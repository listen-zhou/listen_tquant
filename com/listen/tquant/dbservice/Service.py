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
            #self.conn.autocommit(True)
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

    def insert(self, upsert_sql):
        if upsert_sql:
            print(upsert_sql)
            self.cursor.execute(upsert_sql)
            return True
        else:
            return False

    def insert_many(self, upsert_sql_list):
        if upsert_sql_list:
            for upsert_sql in upsert_sql_list:
                try:
                    self.cursor.execute(upsert_sql)
                except:
                    self.conn.rollback()
                    return False
            self.conn.commit()
            return True
        return False