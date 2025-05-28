import logging
import datetime as dt
import sys
from src.day_manager import DayManager
from src.utils.config import *
from src.data_webscrap import GBGB_webscrapper


formatter = logging.Formatter('%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s')
def setup_logger(name, log_file, level=logging.INFO):
    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger
logger = setup_logger('main_logger', f'logs/log_{dt.datetime.now().strftime("%Y%m%d")}.log')


def main():
    # Config logger
    logger.info('started')
    # Update database with latest data if before 8am
    if dt.datetime.now().time() < dt.time(23, 0):
        ws = GBGB_webscrapper()
        ws.update_db()

    # Plan the day
    dm = DayManager(env=ENV)
    dm.schedule_day()


if __name__ == '__main__':
    sys.exit(int(main() or 0))

