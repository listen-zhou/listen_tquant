# -*- coding: utf-8 -*-

import pymysql
import configparser
import os


class DbService(object):
    def __init__(self):
        # charset必须设置为utf8，而不能为utf-8
        config = configparser.ConfigParser()
        os.chdir('../config')
        file_path = os.getcwd() + '\config.ini'
        print(file_path)
        config.read(file_path)
        mysql_section = config['mysql']
        if mysql_section:
            host = mysql_section['db.host']
            port = int(mysql_section['db.port'])
            username = mysql_section['db.username']
            password = mysql_section['db.password']
            dbname = mysql_section['db.dbname']
            charset = mysql_section['db.charset']
            self.conn = pymysql.connect(host=host, port=port, user=username, passwd=password, db=dbname, charset=charset)
            self.cursor = self.conn.cursor()
        else:
            raise FileNotFoundError('config.ini mysql section not found!!!')

    # 数据库连接关闭
    def close(self):
        if self.cursor:
            self.cursor.close()
            print('---> 关闭游标')
        if self.conn:
            self.conn.close()
            print('---> 关闭连接')

    # noinspection SpellCheckingInspection
    def insert(self, upsert_sql):
        if upsert_sql:
            self.cursor.execute(upsert_sql)
            return True
        else:
            return False

    # noinspection SpellCheckingInspection,PyBroadException
    def insert_many(self, upsert_sql_list):
        if upsert_sql_list:
            for upsert_sql in upsert_sql_list:
                try:
                    self.cursor.execute(upsert_sql)
                except Exception:
                    self.conn.rollback()
                    return False
            self.conn.commit()
            return True
        return False

    def query(self, query_sql):
        if query_sql:
            self.cursor.execute(query_sql)
            stock_tuple_tuple = self.cursor.fetchall()
            return stock_tuple_tuple
        else:
            return None

