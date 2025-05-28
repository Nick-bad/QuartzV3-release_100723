import os
import datetime as dt
import time
import src.utils.emails_interface as emails
import src.betfair_source as bf
from apscheduler.schedulers.background import BlockingScheduler
from src.utils.config import *
from src.utils import utils
import pandas as pd
import src.ai_model as ai_model
from src.utils.dropbox_interface import DropBoxStore
from src.utils.mysql_interface import GreyhoundData
from src.utils.utils import exception_handler
import pickle as pkl
import logging
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
logger = logging.getLogger('main_logger')


class DayManager:

    def __init__(self, env: str):
        self.env = env
        self.source = bf.Betfair(env=env)
        self.mg = emails.MailGunEmail(env=env)
        self.model = ai_model.GreyhoundsModel(data_source=self.source)

    def schedule_day(self) -> None:
        logger.info('Enter day_manager.schedule_day')
        market_ids = []
        # trick to ensure streaming is started OK
        while not market_ids:
            market_ids = self.source.get_market_books()
            logger.error('Cannot connect to Betfair. Try again in 2 seconds')
            time.sleep(2)

        # Precalc stats for the day
        self.model.calculate_static_data()

        # Add today's active markets to scheduler
        self.markets = self.source.get_market_description()  # only return markets that aren't closed for today
        self.scheduler = BlockingScheduler(executors={'default': ThreadPoolExecutor(1)})  # max 1 job in parallel
        race_ids = set([race.market_id for _, race in self.markets.iterrows() if race.track in TRACKS])
        for race_id in race_ids:
            rm = RaceManager(source=self.source,
                             market_id=race_id,
                             seconds_lag=SECONDS_LAG,
                             emails=self.mg,
                             model=self.model)
            rm.schedule_race(scheduler=self.scheduler)
        self.scheduler.start()

        logger.info(f'{len(self.markets)} jobs scheduled - Exit day_manager.schedule_day')
        self.mg.send_text_email(f'Quartz V3 setup for today - {self.env}', f'{len(market_ids)} jobs scheduled')


class RaceManager:

    def __init__(self,
                 source: bf.Betfair,
                 market_id: str,
                 seconds_lag: int,
                 emails,
                 model):
        self.source = source
        self.market_id = market_id
        self.market_book = self.source.get_market_books(market_ids=[market_id])[0]
        self.emails = emails
        self.seconds_lag = seconds_lag
        self.model = model
        self.dropbox_store = DropBoxStore(env=self.source.env)

        if not self.market_book:
            raise ValueError(f'Quartz V3 found no open market book for market_id= {self.market_id}')
        self.off_time = utils.localize(self.market_book.market_definition.market_time)
        self.job_start_time = self.off_time - dt.timedelta(seconds=self.seconds_lag)

    def schedule_race(self, scheduler: BlockingScheduler):
        logger.info(f'Add job {self.market_id} to scheduler. Start at {self.job_start_time.strftime("%H:%M:%S")}')
        scheduler.add_job(self._manage_race, trigger='date', run_date=self.job_start_time, misfire_grace_time=60 * 10)
        logger.info(f'Added job {self.market_id} to scheduler. Start at {self.job_start_time.strftime("%H:%M:%S")}')

    @exception_handler
    def _manage_race(self):
        # build dummy race card for today
        logger.info(f'Enter day_manager._manage_race for {self.market_id}')
        quartz = self.model.calculate_quartz(market_id=self.market_id)
        logger.info(f'Calculated quartz for {self.market_id}: {quartz[["selection_id", "quartz_pred"]]}')
        self.dropbox_store.publish_quartz(df=quartz[COLUMNS_TO_PUBLISH])
        logger.info(f'Exit day_manager._manage_race for {self.market_id}')
