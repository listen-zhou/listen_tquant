# conding: utf-8

import decimal
from decimal import Decimal
from decimal import getcontext
import copy

class Utils():

    @staticmethod
    def format_log_message(list):
        if list is not None and len(list) > 0:
            length = len(list)
            i = 0
            message = ''
            while i < length:
                message += '{0[' + str(i) + ']} '
                i += 1
            message = message.format(list)
            return message
        else:
            return None

    @staticmethod
    def quotes_surround(str):
        if str is not None:
            return "'" + str + "'"
        return str

    @staticmethod
    def base_round(val, n):
        if val is not None:
            val = Decimal(val, getcontext())
            return val.__round__(n)
        return None
    @staticmethod
    def base_round_zero(val, n):
        if val is not None:
            val = Decimal(val, getcontext())
        else:
            val = Decimal(0, getcontext())
        return val.__round__(n)

    @staticmethod
    def division(divisor, dividend):
        if divisor is not None and dividend is not None and dividend != 0 and dividend != Decimal(0):
            return divisor / dividend
        return None

    @staticmethod
    def division_zero(divisor, dividend):
        if divisor is not None:
            if dividend is not None and dividend != 0 and dividend != Decimal(0):
                return divisor / dividend
        return Decimal(0)

    @staticmethod
    def sum(list):
        if list is not None and len(list) > 0:
            total = Decimal(0)
            for item in list:
                if item is not None:
                    total += item
            return total
        return None

    @staticmethod
    def sum_zero(list):
        if list is not None and len(list) > 0:
            total = Decimal(0)
            for item in list:
                if item is not None:
                    total += item
            return total
        return Decimal(0)

    @staticmethod
    def average(list):
        if list is not None and len(list) > 0:
            total = Utils.sum(list)
            if total is not None:
                average = total / Decimal(len(list))
                return average
        return None

    @staticmethod
    def average_zero(list):
        if list is not None and len(list) > 0:
            total = Utils.sum(list)
            if total is not None:
                average = total / Decimal(len(list))
                return average
        return Decimal(0)

    @staticmethod
    def deepcopy_list(list):
        return copy.deepcopy(list)

