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

FOUR_MONTH_INTERVAL = 60 * 60 * 24 * 7 * 4 * 4
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


    def find_starting_timestamp_for_new_pairs(self) -> dict[str, int] | None:
        """
        Finds the first trading minute of a pair with minimal API calls.
        Uses large intervals first, then binary search for precision.
        Returns a dictionary {market_symbol: first_trade_timestamp}.
        """
        pairs = DataHelper.retrieve_pairs_without_start_timestamp
        starting_timestamps = {}
        for market_symbol in pairs:
            print(f"Searching for starting timestamp of {market_symbol}")
            low = 1726292210  # 2024-09-14 09:36:50+03 (earliest known complete data)
            high = self.cur_unix_time
            step = FOUR_WEEK_INTERVAL  # Start with large steps
            # Step 1: Find the first non-empty region using exponential search
            while low < high:
                results = self.get_candle(market_symbol, low)
                if results:
                    break  # Found a valid region, now narrow down
                low += step  # Move forward in time
                step = max(step // 2, ONE_DAY_INTERVAL)  # Reduce step gradually
            # Step 2: Binary search within the found non-empty region
            high = low  # Now high is the first known valid timestamp
            low -= step  # Step back a bit to make sure we include the first trade
            while low < high:
                mid = (low + high) // 2
                results = self.get_candle(market_symbol, mid)
                if results:
                    high = mid  # Move left to find earlier candles
                else:
                    low = mid + ONE_MINUTE_INTERVAL  # Move right
            # Final validation: Store the first non-empty timestamp
            results = self.get_candle(market_symbol, low)
            if results:
                starting_timestamps[market_symbol] = results[0]["timestamp"]
                print(f"Starting timestamp: {results[0]["timestamp"]}")
        return starting_timestamps if starting_timestamps else None


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
