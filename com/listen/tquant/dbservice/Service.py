# -*- coding: utf-8 -*-

import pymysql
import configparser
import os
import traceback

class DbService(object):
    def __init__(self):
        # charset必须设置为utf8，而不能为utf-8
        config = configparser.ConfigParser()
        os.chdir('../config')
        file_path = os.getcwd() + '\config.cfg'
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
        try:
            if upsert_sql:
                self.cursor.execute(upsert_sql)
                return True
            else:
                return False
        except Exception:
            traceback.print_exc()

    # noinspection SpellCheckingInspection,PyBroadException
    def insert_many(self, upsert_sql_list):
        try:
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
        except Exception:
            self.conn.rollback()
            traceback.print_exc()

    def query(self, query_sql):
        try:
            if query_sql:
                self.cursor.execute(query_sql)
                stock_tuple_tuple = self.cursor.fetchall()
                return stock_tuple_tuple
            else:
                return None
        except Exception:
            traceback.print_exc()

    # 证券数据处理进度表upsert操作
    def upsert_tquant_security_process_progress(self, data_dict):
        if data_dict:
            sql = "insert into tquant_security_process_progress (" \
                  "business_type, security_code, security_type, exchange_code, process_progress" \
                  ") " \
                  "values (" \
                  "{business_type}, {security_code}, {security_type}, {exchange_code}, {process_progress}" \
                  ") " \
                  "on duplicate key update " \
                  "process_progress=values(process_progress)"
            try:
                sql = sql.format(
                    business_type="'" + data_dict['business_type'] + "'",
                    security_code="'" + data_dict['security_code'] + "'",
                    security_type="'" + data_dict['security_type'] + "'",
                    exchange_code="'" + data_dict['exchange_code'] + "'",
                    process_progress=data_dict['process_progress']
                )
                self.cursor.execute(sql)
            except Exception:
                traceback.print_exc()

