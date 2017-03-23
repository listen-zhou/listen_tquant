# coding: utf-8

import logging
import logging.config
import os


class Logger():
    def __init__(self):
        os.chdir('../config')
        logging.config.fileConfig('logger.cfg')
        self.log = logging.getLogger('log')

    def info(self, message, list):
        if list:
            message = message.format(list)
        self.log.info(message)