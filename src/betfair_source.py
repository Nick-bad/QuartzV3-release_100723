# -*- coding: utf-8 -*-
import queue
import threading
from tenacity import retry, wait_exponential
import betfairlightweight
from betfairlightweight import StreamListener
from betfairlightweight import BetfairError
from betfairlightweight.filters import streaming_market_filter, streaming_market_data_filter, market_filter
from typing import Dict, List, Optional
import pandas as pd
from src.utils.utils import *
from src.utils.emails_interface import MailGunEmail
from src.utils.config import *
import src.utils.utils as utils
import re


logger = logging.getLogger('main_logger')


class Streaming(threading.Thread):
    def __init__(
        self,
        client: betfairlightweight.APIClient,
        market_filter: dict,
        market_data_filter: dict,
        conflate_ms: int = 100,
        streaming_unique_id: int = 1000,
    ):
        threading.Thread.__init__(self, daemon=True, name=self.__class__.__name__)
        self.client = client
        self.market_filter = market_filter
        self.market_data_filter = market_data_filter
        self.conflate_ms = conflate_ms
        self.streaming_unique_id = streaming_unique_id
        self.stream = None
        self.output_queue = queue.Queue()
        self.listener = StreamListener(output_queue=self.output_queue, max_latency=5)

    @retry(wait=wait_exponential(multiplier=1, min=2, max=20))
    def run(self) -> None:
        logger.info("Starting MarketStreaming")
        self.client.login_interactive()
        self.stream = self.client.streaming.create_stream(
            unique_id=self.streaming_unique_id, listener=self.listener
        )
        try:
            self.streaming_unique_id = self.stream.subscribe_to_markets(
                market_filter=self.market_filter,
                market_data_filter=self.market_data_filter,
                conflate_ms=self.conflate_ms,
                initial_clk=self.listener.initial_clk,  # supplying these two values allows a reconnect
                clk=self.listener.clk,
            )
            self.stream.start()
        except BetfairError:
            logger.error("MarketStreaming run error", exc_info=True)
            raise
        except Exception:
            logger.critical("MarketStreaming run error", exc_info=True)
            raise
        logger.info("Stopped MarketStreaming {0}".format(self.streaming_unique_id))

    def stop(self) -> None:
        if self.stream:
            self.stream.stop()


class Betfair:
    def __init__(self, env: str):
        self.env = env
        self.trading = betfairlightweight.APIClient("### LOGIN ###",
                                                    BETFAIR_PASSWORD,
                                                    app_key=BETFAIR_APP_KEY)

        market_filter = streaming_market_filter(event_type_ids=["4339"], country_codes=["GB"], market_types=["WIN"])
        market_data_filter = streaming_market_data_filter(fields=["EX_BEST_OFFERS", "EX_MARKET_DEF", "EX_TRADED"],
                                                          ladder_levels=3)
        self.streaming = Streaming(self.trading, market_filter, market_data_filter)
        self.streaming.start()
        self.emails = MailGunEmail(env=self.env)
        self.results = None
        self.records = {}

    def get_market_books(self, market_ids: List[str] = []):
        '''
        Get market book and logs the input from Betfair
        :param market_ids:
        :return:
        '''
        if not market_ids:
            return self.streaming.listener.snap()
        else:
            return self.streaming.listener.snap(market_ids=market_ids)

    def get_market_description(self, market_id: Optional[str]=None) -> pd.DataFrame:
        '''
        Get list of races for today as a DataFrame with columns='market_id', 'track', 'start_time_utc', 'grade_raw', 'grade', 'distance', 'trap', 'dog_name', 'selection_id', 'status'
        :return:
        '''
        if market_id:
            mkt_catalogue = self.trading.betting.list_market_catalogue(market_filter(event_type_ids=["4339"],
                                                                                     market_countries=["GB"],
                                                                                     market_type_codes=["WIN"],
                                                                                     market_ids=[market_id]),
                                                                       market_projection=['MARKET_DESCRIPTION',
                                                                                          'RUNNER_DESCRIPTION',
                                                                                          'EVENT'],
                                                                       max_results=1000)
            markets = self.get_market_books(market_ids=[market_id])
        else:
            mkt_catalogue = self.trading.betting.list_market_catalogue(market_filter(event_type_ids=["4339"],
                                                                                    market_countries=["GB"],
                                                                                    market_type_codes=["WIN"]),
                                                                       market_projection=['MARKET_DESCRIPTION',
                                                                                          'RUNNER_DESCRIPTION',
                                                                                          'EVENT'],
                                                                       max_results=1000)
            markets = self.get_market_books()
        def _get_grade(g):
            grade_raw = g[0 : g.find(' ')]
            if grade_raw not in ALLOWED_GRADES:
                return 'NOT_ALLOWED'
            if grade_raw == 'OR':
                return 0
            if grade_raw == 'OR1':
                return 1
            if grade_raw == 'OR2':
                return 2
            if grade_raw == 'OR3':
                return 3
            else:
                return int(re.sub("[^0-9]", "", grade_raw))
        def _is_or(g):
            if  g[0: g.find(' ')] in ['OR', 'OR1', 'OR2', 'OR3']:
                return 1
            else:
                return 0
        def _get_distance(s):
            return s[s.find(' ') + 1: -1].strip()
        def _get_dog_name(s):
            s = s[s.find('.') + 1 :].strip()
            return clean_dog_name(s)
        def _get_trap(s):
            s = s[0: s.find('.')]
            return int(s.strip())
        mkt_catalogue = [[m.market_id, m.market_name, _get_grade(m.market_name), _get_distance(m.market_name), r.selection_id, _get_dog_name(r.runner_name), _get_trap(r.runner_name), _is_or(m.market_name)] for m in mkt_catalogue for r in m.runners]
        mkt_catalogue = pd.DataFrame(mkt_catalogue, columns=['market_id', 'grade_raw', 'grade', 'distance', 'selection_id', 'dog_name', 'trap', 'is_or'])
        markets = [[m.market_id, m.market_definition.venue, m.market_definition.market_time, r.selection_id, r.status] for m in markets for r in m.runners]
        markets = pd.DataFrame(data=markets, columns=['market_id', 'track', 'start_time_utc', 'selection_id', 'status'])
        df = pd.merge(markets, mkt_catalogue, left_on=['market_id', 'selection_id'], right_on=['market_id', 'selection_id'])
        df['start_time_loc'] = df['start_time_utc'].apply(lambda x: utils.localize(x))
        df = df[df['start_time_loc'].dt.date == dt.datetime.utcnow().date()]  # only keep today's events
        return df[df['grade'] != 'NOT_ALLOWED']

    def record_prices(self):
        # get books to record
        books = [b for b in self.get_market_books() if b.status == 'ACTIVE' and b.market_definition.market_time - dt.timedelta(seconds=RECORD_DELAY) <= dt.datetime.utcnow()]
        # store the record
        for b in books:
            data = {
                'dt': dt.datetime.timestamp(),
                'r': {r['selection_id']: r['ex'] for r in b._data['runners'] if r['status'] == 'ACTIVE'}
            }
            if b.market_id not in self.records:
                self.records[b.market_id] = [data]
            else:
                self.records[b.market_id].append(data)
        # archive records if the market is closed
        for r in self.records:
            if r not in books:
                # archive the market data
                raise NotImplementedError
                # delete from records
                self.records.pop(r.market_id)

if __name__ == '__main__':
    ''' just for on the fly test '''
    import time
    source = Betfair(env='UAT')
    time.sleep(20)
    df = source.get_market_description(market_id='1.214989649')
    print(df)

