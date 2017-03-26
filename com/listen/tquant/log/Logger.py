# coding: utf-8

import logging
import logging.config
import os


class Logger():
    def __init__(self):
        os.chdir('../config')
        logging.config.fileConfig('logger.cfg')
        self.log = logging.getLogger('log')

    def debug(self, message, list):
        if list:
            message = message.format(list)
        self.log.debug(message)

    def info(self, message, list):
        if list:
            message = message.format(list)
        self.log.info(message)

    def warn(self, message, list):
        if list:
            message = message.format(list)
        self.log.warn(message)

    def error(self, message, list):
        if list:
            message = message.format(list)
        self.log.error(message)

    def exception(self, message, list):
        if list:
            message = message.format(list)
            self.log.error(message)
            logging.exception(message)