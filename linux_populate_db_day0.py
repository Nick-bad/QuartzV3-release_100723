'''
Script to be used to populate the database on day 1
'''
import os
import sys
from src.data_webscrap import GBGB_webscrapper
import logging
import datetime as dt

formatter = logging.Formatter('%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s')
def setup_logger(name, log_file, level=logging.INFO):
    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger
logger = setup_logger('db_construction_day0_logger', f'logs/log_{dt.datetime.now().strftime("%Y%m%d")}.log')


def main():
    # change path on Linux
    if os.name != 'nt':
        os.chdir('/home/nicolas/Documents/tipstronic_backend-master/')
    ws = GBGB_webscrapper()
    #ws.update_db(330000, 330200)
    logger.info('load db from scratch: initial test done')
    print('Initial test done')
    #ws.update_db(280000, 300000)
    #ws.update_db(300000, 330000)
    #ws.update_db(330000, 360000)
    ws.update_db(360000, 397000)


if __name__ == "__main__":
    sys.exit(int(main() or 0))
    print('it is finished')