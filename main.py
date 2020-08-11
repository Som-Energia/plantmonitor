# -*- coding: utf-8 -*-
from conf.logging_configuration import LOGGING

import logging
import logging.config
logging.config.dictConfig(LOGGING)
logger = logging.getLogger("plantmonitor")

from conf.startup_configuration import add_jobs,build_app


def main():

    app = build_app()
    add_jobs(app)

    try:
        logger.info("Running plant monitor...")
        app.start()
    except (KeyboardInterrupt, SystemError):
        logger.info("Stopping plant monitor...")
        app.shutdown()
    except Exception as e:
        msg = "An uncontroled excepction occured: %s"
        logger.exception(msg, e)


if __name__ == '__main__':
    main()
