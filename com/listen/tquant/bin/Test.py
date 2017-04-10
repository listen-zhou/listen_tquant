# coding: utf-8
import inspect
import tquant as tt

class Test:
    @staticmethod
    def get_alive(security_code):
        data = tt.get_stock_bar(security_code, 1)
        return data

    @staticmethod
    def get_last_tick(security_codes):
        data = tt.get_(security_codes)

print(Test.get_last_tick(['002466']))