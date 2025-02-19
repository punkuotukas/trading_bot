"""
This module cotains functions that execute initial comparison between local
database and Bitstamp API provided data
"""

import datetime
import time
import pandas as pd
from dotenv import load_dotenv
import requests
import pytz
from .data_helper import DataHelper

load_dotenv()

API_URL = "https://www.bitstamp.net/api/v2/trading-pairs-info/"
cur_time = datetime.datetime.now(pytz.timezone(
    "Europe/Vilnius")).replace(microsecond=0)
cur_unix_time = int(time.mktime(cur_time.timetuple()))


def check_for_new_pairs() -> None:
    """
    if API returns traded pairs that do not exist on DB,
    adds them to DB.
    Plus, updates DB table for las_checked_for_trading column
    for all pairs that API returned as traded
    """
    db_df = DataHelper.retrieve_trading_status_from_db
    # pylint: disable=unsubscriptable-object
    enabled_db_pairs = db_df.loc[db_df["status"] is True, "pairs"].to_list()
    resp = requests.get(API_URL, timeout=(3, None))
    api_results = resp.json()
    api_pairs_list = [pair["url_symbol"]
                      for pair in api_results if pair is not None]
    if len(api_pairs_list) > len(enabled_db_pairs):
        new_pairs_list: list[str] = []
        print("New trading pairs have been launched")
        new_pairs = set(api_pairs_list) - set(enabled_db_pairs)
        print("New pairs are the following:")
        print(new_pairs)
        for pair in api_results:
            DataHelper().update_check_time(pair)
            if pair["url_symbol"] in new_pairs:
                new_pairs_list.append(pair)
        DataHelper().insert_new_pairs_to_main_table(new_pairs_list)

def update_disabled_pairs() -> None:
    """
    existing pairs in database are checked for trading status on API
    - if enabled on DB and API does not return any data for specific pair,
    disable trading status function is executed.
    Plus, last_checked_for_trading column value is updated with current time
    """
    existing_pairs = DataHelper.retrieve_trading_status_from_db
    api_pairs: list[str] = []
    api_data = requests.get(url=API_URL, timeout=(3, None)).json()
    for pair in api_data:
        api_pairs.append(pair["url_symbol"])
    # pylint: disable=unsubscriptable-object
    disabled_pairs: pd.DataFrame = existing_pairs.loc[~existing_pairs['pair_url'].isin(api_pairs),
    ['pair_url', 'trading_enabled', 'last_checked_for_trading']]
    for pair in disabled_pairs['pair_url']:
        DataHelper().disable_trading_on_db(pair)
        DataHelper().update_check_time(pair)
