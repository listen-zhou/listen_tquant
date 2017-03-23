# coding: utf-8

import inspect
from com.listen.tquant.log.Logger import Logger

class BaseService():
    def __init__(self, logger):
        self.logger = logger
        self.serviceName = 'BaseService'

    def base_info(self, message, list):
        self.logger.info(self.get_clsss_name() + ' ' + message, list)

    def base_warn(self, message, list):
        self.logger.warn(self.get_clsss_name() + ' ' + message, list)

    def base_error(self, message, list):
        self.logger.error(self.get_clsss_name() + ' ' + message, list)

    def get_current_method_name(self):
        return inspect.stack()[1][3]

    def get_clsss_name(self):
        return self.__class__.__name__