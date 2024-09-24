import requests
import time
import datetime
import pytz

cur_time = datetime.datetime.now(pytz.timezone("Europe/Vilnius")).replace(microsecond=0)
cur_unix_time = int(time.time())
four_week_interval = 60 * 60 * 24 * 7 * 4
one_week_interval = 60 * 60 * 24 * 7
one_day_interval = 60 * 60 * 24
one_hour_interval = 60 * 60
one_minute_interval = 60


def get_candles(market_symbol, start, limit=1):
    ohlc_url = f"https://www.bitstamp.net/api/v2/ohlc/{market_symbol}/"
    params = {"step": 60, "limit": limit, "start": start}
    resp = requests.get(ohlc_url, params=params)
    results = resp.json()
    return results["data"]["ohlc"]


def find_starting_timestamp(market_symbol):
    start = cur_unix_time - four_week_interval
    results = get_candles(market_symbol, start)
    while results:  # * empty python list is considered falsy
        start = start - four_week_interval
        results = get_candles(market_symbol, start)
        if not results:
            while not results:
                start = start + one_week_interval
                results = get_candles(market_symbol, start)
                if results:
                    while results:
                        start = start - one_day_interval
                        results = get_candles(market_symbol, start)
                        if not results:
                            while not results:
                                start = start + one_hour_interval
                                results = get_candles(market_symbol, start)
                                if results:
                                    while results:
                                        start = start - one_minute_interval
                                        results = get_candles(market_symbol, start)
                                        if not results:
                                            while not results:
                                                start = start + one_minute_interval
                                                results = get_candles(
                                                    market_symbol, start
                                                )
                                                if results:
                                                    start_timestamp = results[0][
                                                        "timestamp"
                                                    ]
                                                    return start_timestamp


pair = "icpusd"
start_timestamp = find_starting_timestamp(pair)
print(f"First trading timestamp for {pair}: {start_timestamp}")
