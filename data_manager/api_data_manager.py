"""
when launched, module collects new ohlc data from API
as new historical data becomes available with time passing
"""

import os
from datetime import datetime
import requests
from dotenv import load_dotenv
import pandas as pd
from data_manager.data_helper import DataHelper

load_dotenv()

curr_time: datetime = datetime.now()
curr_unix_time: int = int(datetime.timestamp(curr_time))
# pylint: disable=unnecessary-lambda-assignment
# pylint: disable=unnecessary-lambda
# lambdas are assigned to variables in this case because
# it allows the use of "apply" for pandas dataframes
to_int = lambda x: int(x)
to_float = lambda x: float(x)
to_timestamp = lambda x: pd.Timestamp(x, unit="s", tz="Europe/Vilnius")
to_unix = lambda x: int(datetime.timestamp(x))

OHLC_URL="https://www.bitstamp.net/api/v2/ohlc/{pair_url}/"

url_dict = {
    "username": os.getenv("PSQL_USER"),
    "password": os.getenv("PSQL_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "database": os.getenv("DB_NAME"),
}

psycopg_conn_str = f"""dbname={url_dict['database']}
                       user={url_dict['username']}
                       password={url_dict['password']}
                       host={url_dict['host']}
                       port={url_dict['port']}"""


def get_new_candles(pair) -> pd.DataFrame|None:
    """
    placeholder
    """
    start_df = DataHelper().retrieve_df_with_last_candle(pair)
    start: int = start_df["unix_timestamp"].values[0] + 60
    ohlc_list: list[dict] = []
    while start < curr_unix_time - 60:
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
            print(f"Bad status code for {pair}")
            if resp.status_code == 404:
                print(f"Setting pair {pair} trading status to \
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


def update_candles_for_existing_pairs():
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
        new_ohlc_df = get_new_candles(pair=pair_url)
        if new_ohlc_df is not None:
            new_ohlc_df["high"] = new_ohlc_df["high"].apply(to_float)
            new_ohlc_df["open"] = new_ohlc_df["open"].apply(to_float)
            new_ohlc_df["low"] = new_ohlc_df["low"].apply(to_float)
            new_ohlc_df["close"] = new_ohlc_df["close"].apply(to_float)
            new_ohlc_df["volume"] = new_ohlc_df["volume"].apply(to_float)
            new_ohlc_df["unique_pair_id"] = row[1].values[0]
            print(f"Updating database for: {row[1].values[0]} {pair_url}")
            DataHelper().insert_candles_to_db(new_ohlc_df, pair_url)
            DataHelper().update_check_time(pair_url)
        else:
            continue
