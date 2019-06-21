# -*- coding: utf-8 -*-
import logging
from conf.startup_configuration import add_jobs,build_app


def main():

    app = build_app()
    add_jobs(app)

    try:
        logging.info("Running plant monitor...")
        app.start()
    except (KeyboardInterrupt, SystemError):
        logging.info("Stopping plant monitor...")
        app.shutdown()
    except Exception as e:
        msg = "An uncontroled excepction occured: %s"
        logging.exception(msg, e)


if __name__ == '__main__':
    main()