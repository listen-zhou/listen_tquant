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


class Service():
    def get_method_name(self):
        return inspect.stack()[1][3]

    def get_classs_name(self):
        return self.__class__.__name__
