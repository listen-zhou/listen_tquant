# coding: utf-8

import inspect
from decimal import *
import decimal
import threading

import logging

from com.listen.tquant.log import Logger
import copy

context = decimal.getcontext()
context.rounding = decimal.ROUND_05UP


class BaseService():
    def __init__(self, logger):
        self.logger = logger

    def deepcopy_list(slef, list):
        return copy.deepcopy(list)

    def base_exception(self, list):
        self.logger.exception(list)

    def get_method_name(self):
        return inspect.stack()[1][3]

    def get_clsss_name(self):
        return self.__class__.__name__

    def sum(self, list):
        if list is not None and len(list) > 0:
            total = Decimal(0)
            for item in list:
                if item is not None:
                    total += item
            return total
        return None

    def average(self, list):
        if list is not None and len(list) > 0:
            total = self.sum(list)
            if total is not None:
                average = total / Decimal(len(list))
                return average
            return None
        return None

    def base_round(self, val, n):
        if val is not None:
            val = Decimal(val, getcontext())
            return val.__round__(n)
        return None

    def quotes_surround(self, str):
        if str is not None:
            return "'" + str + "'"
        return str

