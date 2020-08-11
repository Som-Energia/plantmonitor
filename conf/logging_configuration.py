# -*- coding: utf-8 -*-
LOGGING = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'verbose': {
            'format': '[%(asctime)s] [%(levelname)s]'
                      '[%(module)s.%(funcName)s:%(lineno)s] %(message)s'
        },
    },
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'verbose'
        },
    },
    'loggers': {
        'plantmonitor': {
            'level': 'DEBUG',
            'propagate': True,
            'handlers': ['console'],
        },
        'test': {
            'level': 'DEBUG',
            'propagate': True,
            'handlers': ['console'],
        },
        'zeep.transports': {
            'level': 'INFO',
            'propagate': True,
            'handlers': ['console'],
        },
    }
}
