# coding: utf-8

import inspect
from decimal import *
import decimal
import threading

import logging

import datetime

from com.listen.tquant.log import Logger
import copy

context = decimal.getcontext()
context.rounding = decimal.ROUND_05UP


class Service():
    def get_method_name(self):
        return inspect.stack()[1][3]

    def get_classs_name(self):
        return self.__class__.__name__

    def now(self):
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def info(self):
        return 'INFO'

    def error(self):
        return 'ERROR'

    def warn(self):
        return 'WARN'

    def format_list(self, log_list):
        if log_list:
            length = len(log_list)
            i = 0
            message = ''
            while i < length:
                message += '{0[' + str(i) + ']} '
                i += 1
            message = message.format(log_list)
            return message
        return ''

    def print_log(self, log_list):
        message = self.format_list(log_list)
        print(message)
