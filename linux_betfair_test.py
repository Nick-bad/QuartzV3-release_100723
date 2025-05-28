import logging
import queue
import threading
import time
import datetime as dt
import betfairlightweight
from betfairlightweight.filters import (
    streaming_market_filter,
    streaming_market_data_filter,
    market_filter
)



# setup logging
logging.basicConfig(filename=f'logs/bf_test_log_{dt.datetime.now().strftime("%Y%m%d")}.log', level=logging.INFO)  # change to DEBUG to see log all updates

# create trading instance (app key must be activated for streaming)
trading = betfairlightweight.APIClient("###email###",
                                       "###password###",
                                       app_key="###key###")

# login
trading.login_interactive()

# create queue
output_queue = queue.Queue()

# create stream listener
listener = betfairlightweight.StreamListener(output_queue=output_queue)

# create stream
stream = trading.streaming.create_stream(listener=listener)

# create filters (GB WIN racing)
my_market_filter = streaming_market_filter(
    event_type_ids=["4339"], country_codes=["GB"], market_types=["WIN"], 
)
market_data_filter = streaming_market_data_filter(
    fields=["EX_BEST_OFFERS", "EX_MARKET_DEF", "EX_TRADED"], ladder_levels=3
)

# subscribe
streaming_unique_id = stream.subscribe_to_markets(
    market_filter=my_market_filter,
    market_data_filter=market_data_filter,
    conflate_ms=1000,  # send update every 1000ms
)



# start stream in a new thread (in production would need err handling)
t = threading.Thread(target=stream.start, daemon=True)
t.start()

time.sleep(10)

m = trading.betting.list_market_catalogue(market_filter(event_type_ids=["4339"], 
                                                        market_countries=["GB"], 
                                                        market_type_codes=["WIN"]),
                                          market_projection=["MARKET_DESCRIPTION", "RUNNER_DESCRIPTION"],
                                          max_results=1000)
market_books = listener.snap()