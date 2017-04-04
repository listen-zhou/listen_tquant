# -*- coding: utf-8 -*-

import pymysql
import configparser
import os
import traceback
import sys

class DbService(object):
    def __init__(self):
        # charset必须设置为utf8，而不能为utf-8
        config = configparser.ConfigParser()
        os.chdir('../config')
        config.read('database.cfg')
        mysql_section = config['mysql']
        if mysql_section:
            host = mysql_section['db.host']
            port = int(mysql_section['db.port'])
            username = mysql_section['db.username']
            password = mysql_section['db.password']
            dbname = mysql_section['db.dbname']
            charset = mysql_section['db.charset']
            self.conn = pymysql.connect(host=host, port=port, user=username, passwd=password, db=dbname, charset=charset)
            self.conn.autocommit(True)
            self.cursor = self.conn.cursor()
        else:
            raise FileNotFoundError('database.cfg mysql section not found!!!')

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
            print('error sql:', upsert_sql)
            traceback.print_exc()

    # noinspection SpellCheckingInspection,PyBroadException
    def insert_many(self, upsert_sql_list):
        try:
            if upsert_sql_list:
                for upsert_sql in upsert_sql_list:
                    try:
                        self.cursor.execute(upsert_sql)
                    except Exception:
                        print('error sql:', upsert_sql)
                        self.conn.rollback()
                        traceback.print_exc()
                        return False
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
            exc_type, exc_value, exc_traceback = sys.exc_info()
            print(exc_type, exc_value, exc_traceback, 'sql error:', query_sql)
            return None

    def query_all_security_codes(self):
        sql = "select security_code, exchange_code " \
              "from tquant_security_info " \
              "where security_type = 'STOCK'"
        return self.query(sql)

    def get_calendar_max_the_date(self):
        """
        查询交易日表中最大交易日日期
        :return:
        """
        sql = "select max(the_date) max_the_date from tquant_calendar_info"
        the_date = self.query(sql)
        if the_date is not None and len(the_date) > 0:
            max_the_date = the_date[0][0]
            return max_the_date
        return None

    def get_batch_list_security_codes(self, batch_size):
        tuple_security_codes = self.query_all_security_codes()
        if tuple_security_codes is not None and len(tuple_security_codes) > 0:
            batch_list = []
            size = len(tuple_security_codes)
            # 分组后余数
            remainder = size % batch_size
            if remainder > 0:
                remainder = 1
            # 分组数，取整数，即批量的倍数
            multiple = size // batch_size
            total = remainder + multiple
            print('size:', size, 'batch:', batch_size, 'remainder:', remainder, 'multiple:', multiple, 'total:', total)
            i = 0
            while i < total:
                # 如果是最后一组，则取全量
                if i == total - 1:
                    temp_tuple = tuple_security_codes[i * batch_size:size]
                else:
                    temp_tuple = tuple_security_codes[i * batch_size:(i + 1) * batch_size]
                batch_list.append(temp_tuple)
                i += 1
            return batch_list
        return None

    def get_batch_list_except_security_codes(self):
        sql = "select c.security_code, c.exchange_code " \
              "from " \
              "( " \
              "select a.security_code, a.exchange_code, a.k_count, b.avg_count ," \
              "b.ma, (a.k_count-b.avg_count + 1) diff " \
              "from " \
              "( " \
              "select security_code, exchange_code, count(*) k_count " \
              "from tquant_stock_day_kline " \
              "group by security_code, exchange_code" \
              ") a " \
              "left join " \
              "( " \
              "select security_code, exchange_code, ma, count(*) avg_count " \
              "from tquant_stock_average_line " \
              "group by security_code, exchange_code, ma" \
              ") b " \
              "on a.security_code = b.security_code and b.exchange_code = a.exchange_code " \
              "having (a.k_count-b.avg_count + 1) != b.ma ) c " \
              "group by c.security_code, c.exchange_code "
        tuple_security_codes = self.query(sql)
        if tuple_security_codes is not None and len(tuple_security_codes) > 0:
            return tuple_security_codes
        return None

    def get_day_kline_except_security_codes(self):
        sql = "select security_code, exchange_code " \
              "from " \
              "( " \
              "select security_code, exchange_code " \
              "from tquant_stock_day_kline " \
              "where close is null or close <= 0 " \
              "or open is null or open <= 0 " \
              "or high <= 0 or high is null " \
              "or low <= 0 or low is null " \
              "group by security_code, exchange_code " \
              ") a"
        tuple_security_codes = self.query(sql)
        if tuple_security_codes is not None and len(tuple_security_codes) > 0:
            return tuple_security_codes
        return None