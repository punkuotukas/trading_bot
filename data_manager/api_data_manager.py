"""
when launched, module collects new ohlc data from API
as new historical data becomes available with time passing
"""

import os
from datetime import datetime
from loguru import logger
import requests
from dotenv import load_dotenv
import pandas as pd
from data_manager.data_helper import DataHelper

load_dotenv()

OHLC_URL="https://www.bitstamp.net/api/v2/ohlc/{pair_url}/"
FOUR_MONTH_INTERVAL = 60 * 60 * 24 * 7 * 4 * 4
FOUR_WEEK_INTERVAL = 60 * 60 * 24 * 7 * 4
ONE_WEEK_INTERVAL = 60 * 60 * 24 * 7
ONE_DAY_INTERVAL = 60 * 60 * 24
ONE_HOUR_INTERVAL = 60 * 60
ONE_MINUTE_INTERVAL = 60

curr_time: datetime = datetime.now()
cur_unix_time: int = int(datetime.timestamp(curr_time))

# DataHelper = DataHelper()

class APIDataManager:
    """
    placeholder
    """
    def __init__(self) -> None:
        # pylint: disable=unnecessary-lambda-assignment
        # pylint: disable=unnecessary-lambda
        # lambdas are assigned to variables in this case because
        # it allows the use of "apply" for pandas dataframes
        self.to_int = lambda x: int(x)
        self.to_float = lambda x: float(x)
        self.to_timestamp = lambda x: pd.Timestamp(x, unit="s", tz="Europe/Vilnius")
        self.to_unix = lambda x: int(datetime.timestamp(x))
        self.url_dict = {
            "username": os.getenv("PSQL_USER"),
            "password": os.getenv("PSQL_PASSWORD"),
            "host": os.getenv("DB_HOST"),
            "port": os.getenv("DB_PORT"),
            "database": os.getenv("DB_NAME"),
        }
        self.psycopg_conn_str = f"""dbname={self.url_dict['database']}
                               user={self.url_dict['username']}
                               password={self.url_dict['password']}
                               host={self.url_dict['host']}
                               port={self.url_dict['port']}"""


    def get_new_candles(self, pair) -> pd.DataFrame|None:
        """
        placeholder
        """
        start_df = DataHelper().retrieve_df_with_last_candle(pair)
        start: int = start_df["unix_timestamp"].values[0]
        ohlc_list: list[dict] = []
        while start < cur_unix_time - 60:
            try:
                resp = requests.get(
                    OHLC_URL.format(pair_url=pair),
                    params={
                        "step": 60,
                        "limit": 1000,
                        "start": start,
                        "exclude_current_candle": True,
                    },
                    timeout=(3, None)
                )
                resp.raise_for_status()
            except requests.exceptions.HTTPError:
                logger.error(f"Bad status code for {pair}")
                if resp.status_code == 404:
                    logger.info(f"Setting pair {pair} trading status to \
                    DISABLED")
                    DataHelper().disable_trading_on_db(pair)
                    DataHelper().update_check_time(pair)
                    return None
            results = resp.json()["data"]["ohlc"]
            ohlc_list.extend(results)
            start = start + 60000
        api_df = pd.DataFrame(ohlc_list)
        api_df["timestamp"] = api_df["timestamp"].astype(int)
        api_df["timestamp"] = api_df["timestamp"].apply(
            lambda x: pd.Timestamp(x, unit="s", tz="Europe/Vilnius"))
        return api_df


    def update_candles_for_existing_pairs(self):
        """
        gets existing pairs from DB where trading_status = Enabled,
        updates last existing candle's timestamp to set start for API call,
        iterates through each pair to retrieve new candles from API,
        updates DB pair tables with new candles,
        updates main table with check time
        """
        pairs_df = DataHelper().retrieve_traded_pairs_from_db
        for row in pairs_df.iterrows():
            pair_url = row[1].values[1]
            new_ohlc_df = self.get_new_candles(pair=pair_url)
            if new_ohlc_df is not None:
                new_ohlc_df["high"] = new_ohlc_df["high"].apply(self.to_float)
                new_ohlc_df["open"] = new_ohlc_df["open"].apply(self.to_float)
                new_ohlc_df["low"] = new_ohlc_df["low"].apply(self.to_float)
                new_ohlc_df["close"] = new_ohlc_df["close"].apply(self.to_float)
                new_ohlc_df["volume"] = new_ohlc_df["volume"].apply(self.to_float)
                new_ohlc_df["unique_pair_id"] = row[1].values[0]
                logger.info(f"Updating database for: {row[1].values[0]} {pair_url}")
                DataHelper().insert_candles_to_db(new_ohlc_df, pair_url)
                DataHelper().update_check_time(pair_url)
            else:
                continue


    def get_candle(self, market_symbol, start, limit=1) -> list[dict]:
        """
         gets single one-minute candle from API
        """
        ohlc_url = OHLC_URL.format(pair_url=market_symbol)
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
        pairs: list[str] = DataHelper().retrieve_pairs_without_start_timestamp
        starting_timestamps = {}
        # pylint: disable=not-an-iterable
        for market_symbol in pairs:
            logger.info(f"Searching for starting timestamp of {market_symbol}")
            low = 1726292210  # 2024-09-14 09:36:50+03 (earliest known complete data)
            high = cur_unix_time
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
            readable_timestamp = datetime.fromtimestamp(int(results[0]["timestamp"]))
            if results:
                starting_timestamps[market_symbol] = results[0]["timestamp"]
                unix_timestamp = results[0]["timestamp"]
                logger.info(f"{market_symbol}: {results[0]["timestamp"]} -> {readable_timestamp}")
                DataHelper().update_start_timestamp_in_main_table(pair=market_symbol,
                                                                  timestamp=readable_timestamp,
                                                                  unix_timestamp=unix_timestamp,
                                                                  connection=self.psycopg_conn_str)
        return starting_timestamps if starting_timestamps else None
