# coding: utf-8

from com.listen.tquant.utils.Utils import Utils

class UtilService():

    @staticmethod
    def get_day_kline_upsertsql(security_code, dict_data, is_exist):
        if is_exist:
            sql = "update tquant_stock_history_quotation " \
                  "set amount = {amount}, vol = {vol}, open={open}, high={high}, low={low}, close = {close} " \
                  "where security_code = {security_code} and the_date = {the_date}"
        else:
            sql = "insert into tquant_stock_history_quotation (security_code, the_date, amount, vol, open, high, low, close) " \
                  "values ({security_code}, {the_date}, {amount}, {vol}, {open}, {high}, {low}, {close}) "
        sql = sql.format(security_code=Utils.quotes_surround(security_code),
                         the_date=Utils.quotes_surround(dict_data['the_date']),
                         amount=dict_data['amount'],
                         vol=dict_data['vol'],
                         open=dict_data['open'],
                         high=dict_data['high'],
                         low=dict_data['low'],
                         close=dict_data['close']
                         )
        return sql

    @staticmethod
    def processing_day_kline_chg_calculate(dict_item1, dict_item2, dict_pre_data):
        """
        计算股票日K涨跌幅指标，即相邻2天的变动幅度
        :param dict_item1: 前一天股票数据，字典类型
        :param dict_item2: 后一天股票数据，字典类型
        :return: 返回后一天股票涨跌幅数据，字典类型
        """
        vol1 = dict_item1['vol']
        vol2 = dict_item2['vol']
        close1 = dict_item1['close']
        close2 = dict_item2['close']
        amount2 = dict_item2['amount']
        vol_chg = Utils.base_round(Utils.division_zero((vol2 - vol1), vol1) * 100, 2)
        close_chg = Utils.base_round(Utils.division_zero((close2 - close1), close1) * 100, 2)
        price_avg = Utils.base_round(Utils.division_zero(amount2, vol2), 2)
        price_avg_pre = dict_pre_data['price_avg_pre']
        if price_avg_pre is not None:
            price_avg_chg = Utils.base_round(Utils.division_zero((price_avg - price_avg_pre), price_avg_pre) * 100, 2)
        else:
            price_avg_chg = 0
        close_price_avg_chg = Utils.base_round_zero(Utils.division_zero(close2 - price_avg, price_avg) * 100, 2)
        open2 = dict_item2['open']
        close_open_chg = Utils.base_round_zero(Utils.division_zero(close2 - open2, open2) * 100, 2)
        return {'vol_chg': vol_chg, 'close_chg': close_chg, 'price_avg': price_avg,
                'price_avg_chg': price_avg_chg, 'close_price_avg_chg': close_price_avg_chg,
                'close_open_chg': close_open_chg}

    @staticmethod
    def get_day_kline_chg_upsertsql(security_code, the_date, dict_data):
        """
        根据计算得出的股票日K涨跌幅指标数据，生成需要更新的sql语句
        :param security_code: 股票代码，字符串类型
        :param the_date: 交易日，日期类型
        :param dict_data: 股票涨跌幅数据，字典类型
        :return: 
        """
        sql = "update tquant_stock_history_quotation set vol_chg = {vol_chg}, " \
              "close_chg = {close_chg}, price_avg = {price_avg}, price_avg_chg = {price_avg_chg}, " \
              "close_price_avg_chg = {close_price_avg_chg}, close_open_chg = {close_open_chg} " \
              "where security_code = {security_code} and the_date = {the_date} "
        sql = sql.format(vol_chg=dict_data['vol_chg'],
                         close_chg=dict_data['close_chg'],
                         price_avg=dict_data['price_avg'],
                         price_avg_chg=dict_data['price_avg_chg'],
                         close_price_avg_chg=dict_data['close_price_avg_chg'],
                         close_open_chg=dict_data['close_open_chg'],
                         security_code=Utils.quotes_surround(security_code),
                         the_date=Utils.quotes_surround(Utils.format_date(the_date)))
        return sql