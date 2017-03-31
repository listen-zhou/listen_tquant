# coding: utf-8

import logging
import logging.config
import os

from logging.handlers import TimedRotatingFileHandler
from logging import Formatter
import os
import sys

class Logger():
    def __init__(self, level, log_path, log_name, when, interval, backupCount):

        if not os.path.exists(log_path):
            try:
                os.makedirs(log_path)
            except Exception:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                print(exc_type, exc_value, exc_traceback)

        format = '%(asctime)s - %(levelname)s - %(message)s'
        formatter = Formatter(format)

        logging.basicConfig(level=level, format=format)

        fileHandler = TimedRotatingFileHandler(filename=log_path + log_name, when=when, interval=interval, backupCount=backupCount)
        fileHandler.setFormatter(formatter)

        # logging.getLogger('') 不会打印root日志，显得美观，而且是重复打印了
        self.log = logging.getLogger('')
        self.log.addHandler(fileHandler)

    def format_list(self, list):
        if list:
            length = len(list)
            i = 0
            message = ''
            while i < length:
                message += '{0[' + str(i) + ']} '
                i += 1
            message = message.format(list)
            return message
        return ''

    def debug(self, list):
        message = self.format_list(list)
        self.log.debug(message)

    def info(self, list):
        message = self.format_list(list)
        self.log.info(message)

    def warn(self, list):
        message = self.format_list(list)
        self.log.warn(message)

    def error(self, list):
        message = self.format_list(list)
        self.log.error(message)

    def exception(self, list):
        message = self.format_list(list)
        self.log.error(message)
        logging.exception(message)