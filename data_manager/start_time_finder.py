"""
this module contains functions when trading start time needs to be determined
for newly added pairs
"""

import time
import datetime
from typing import Dict
import requests
import pytz
import psycopg
from .data_helper import DataHelper

FOUR_WEEK_INTERVAL = 60 * 60 * 24 * 7 * 4
ONE_WEEK_INTERVAL = 60 * 60 * 24 * 7
ONE_DAY_INTERVAL = 60 * 60 * 24
ONE_HOUR_INTERVAL = 60 * 60
ONE_MINUTE_INTERVAL = 60

DataHelper = DataHelper()


class StartTimeFinder:
    """
    When dealing with newly added trading pairs to API,
    it is needed to find the starting timestamp for each pair
    functions in this class deal with searching for it.
    """
    def __init__(self) -> None:
        self.fur_time = datetime.datetime.now(pytz.timezone(
            "Europe/Vilnius")).replace(microsecond=0)
        self.cur_unix_time = int(time.time())

    def get_candle(self, market_symbol, start, limit=1) -> list[Dict]:
        """
         gets single one-minute candle from API
        """
        ohlc_url = f"https://www.bitstamp.net/api/v2/ohlc/{market_symbol}/"
        params = {"step": 60, "limit": limit, "start": start}
        resp = requests.get(ohlc_url, params=params, timeout=(3, None))
        results = resp.json()
        return results["data"]["ohlc"]

    @property
    def find_starting_timestamp_for_new_pairs(self) -> str|None:
        """
        Searches for the precise minute when trading of a pair started
        by narrowing down time window gradually
        """
        pairs = DataHelper.retrieve_pairs_without_start_timestamp
        # pylint: disable=not-an-iterable
        for market_symbol in pairs:
            start: int|None = self.cur_unix_time - ONE_WEEK_INTERVAL
            # Step 1: Find first non-empty result (move forward by one day)
            start  = self.find_non_empty_timestamp(market_symbol,
                                                  start,
                                                  ONE_DAY_INTERVAL,
                                                  increase=True)
            if start is None:
                return None
            # Step 2: Narrow down to the hour
            start = self.find_non_empty_timestamp(market_symbol,
                                                  start,
                                                  ONE_HOUR_INTERVAL,
                                                  increase=False)
            if start is None:
                return None
            # Step 3: Refine to the exact minute
            start = self.find_non_empty_timestamp(market_symbol,
                                                  start,
                                                  ONE_MINUTE_INTERVAL,
                                                  increase=True)
            # Final check: return the timestamp
            results = self.get_candle(market_symbol, start)
        return results[0]["timestamp"] if results else None

    def find_non_empty_timestamp(self,
                                 market_symbol:str,
                                 start:int,
                                 step:int,
                                 increase:bool) -> int|None:
        """
        Moves forward or backward in time
        to find the first non-empty candle result
        """
        results = self.get_candle(market_symbol, start)
        while not results if increase else results:
            start += step if increase else -step
            results: list[Dict] = self.get_candle(market_symbol, start)
        return start if results else None

    def update_start_timestamp_in_main_table(
        self, timestamp, unix_timestamp, connection, pair
    ):
        """
        updates trading start time for specific trading pair
        in the bitstamp_pairs table in DB
        """
        update_start_timestamp_query = """--sql
        UPDATE bitstamp_pairs
        SET start_timestamp = %(timestamp)s,
            unix_timestamp = %(unix_timestamp)s
        WHERE
            pair_url = %(pair_url)s
        """
        # pylint:disable=not-context-manager
        with psycopg.connect(connection) as conn:
            cur = conn.cursor()
            cur.execute(
                update_start_timestamp_query,
                {
                    "timestamp": timestamp,
                    "unix_timestamp": unix_timestamp,
                    "pair_url": pair,
                },
            )
            conn.commit()
            print(f"Start timestamp updated for: {pair}")
            cur.close()
