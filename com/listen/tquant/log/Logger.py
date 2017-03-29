# coding: utf-8

import logging
import logging.config
import os


class Logger():
    def __init__(self):
        os.chdir('../config')
        logging.config.fileConfig('logger.cfg')
        self.log = logging.getLogger('log')

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

    def error(self, message, list):
        message = self.format_list(list)
        self.log.error(message)

    def exception(self, message, list):
        message = self.format_list(list)
        self.log.error(message)
        logging.exception(message)