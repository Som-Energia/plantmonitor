#!/usr/bin/env python3
import os
from importlib import import_module

ENVIRONMENT_VARIABLE = 'PLANTMONITOR_MODULE_SETTINGS'

os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'plantmonitor.conf.settings.devel')


class ImproperlyConfigured(Exception):

    def __init__(self, msg):
        super(ImproperlyConfigured, self).__init__()
        self.msg = msg

    def __repr__(self):
        return self.msg

    def __str__(self):
        return self.__repr__()


class Settings(object):

    def __init__(self, settings_module):
        self.SETTINGS_MODULE = settings_module
        if not self.SETTINGS_MODULE:
            msg = 'Environment variable \"{}\" is not set'
            raise ImproperlyConfigured(msg.format(ENVIRONMENT_VARIABLE))

        mod = import_module(self.SETTINGS_MODULE)
        for setting in dir(mod):
            if setting.isupper():
                setattr(self, setting, getattr(mod, setting))


config = Settings(os.getenv(ENVIRONMENT_VARIABLE))


def configure_logging(logging_config):
    from logging.config import dictConfig

    dictConfig(logging_config)
