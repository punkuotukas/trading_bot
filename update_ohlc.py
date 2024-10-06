import os
import psycopg2
from psycopg2 import sql
import requests
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd
import pytz
from find_start_timestamp import FindStartTimestamp

load_dotenv()

curr_time = datetime.now()
curr_unix_time = int(datetime.timestamp(curr_time))
to_int = lambda x: int(x)
to_float = lambda x: float(x)
to_timestamp = lambda x: pd.Timestamp(x, unit="s", tz="Europe/Vilnius")

url_dict = {
    "username": os.getenv("PSQL_USER"),
    "password": os.getenv("PSQL_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "database": os.getenv("DB_NAME"),
}

psycopg_conn_str = f"dbname={url_dict['database']} user={url_dict['username']} password={url_dict['password']} host={url_dict['host']} port={url_dict['port']}"

sqlalchemy_conn_str = f"postgresql+psycopg://{url_dict['username']}:{url_dict['password']}@{url_dict['host']}:{url_dict['port']}/{url_dict['database']}"


def fetch_pairs_from_db(connection):
    retrieve_pairs_query = """--sql
    SELECT unique_pair_id, pair_url FROM bitstamp_pairs WHERE trading_enabled = TRUE;
    """
    with psycopg2.connect(connection) as conn:
        cur = conn.cursor()
        cur.execute(retrieve_pairs_query)
        results = cur.fetchall()
        cur.close()
    pairs_df = pd.DataFrame(results)
    pairs_df.rename(columns={0: "unique_pair_id", 1: "pair_url"}, inplace=True)
    return pairs_df


def get_pair_latest_candle_timestamp(pair_url, db_connection):
    table_name = f"ohlc_{pair_url}"
    max_timestamp_query = """--sql
    SELECT MAX(timestamp) FROM {table}
    """
    with psycopg2.connect(db_connection) as conn:
        cur = conn.cursor()
        cur.execute(
            sql.SQL(max_timestamp_query).format(table=sql.Identifier(table_name))
        )
        results = cur.fetchone()[0]
        cur.close()
        if results is None:
            finder = FindStartTimestamp()
            results = int(finder.find_starting_timestamp(pair_url, new_pair=True))
            human_datetime = datetime.fromtimestamp(
                results, tz=pytz.timezone("Europe/Vilnius")
            )
            finder.update_start_timestamp_in_main_table(
                human_datetime, results, db_connection, pair_url
            )
            unix_results = results
        else:
            unix_results = int(datetime.timestamp(results))
    return unix_results


def fetch_bitstamp_ohlc(
    market_symbol,
    start,
    step=60,
    limit=1000,
    exclude_current_candle=True,
):
    ohlc_url = f"https://www.bitstamp.net/api/v2/ohlc/{market_symbol}/"
    params = {
        "step": step,
        "limit": limit,
        "start": start,
        "exclude_current_candle": exclude_current_candle,
    }
    resp = requests.get(ohlc_url, params=params)
    api_data = resp.json()["data"]
    ohlc_data = api_data["ohlc"]
    df = pd.DataFrame(ohlc_data)
    df.rename(columns={"timestamp": "unix_timestamp"}, inplace=True)
    df["unix_timestamp"] = df["unix_timestamp"].astype(int)
    df["timestamp"] = df["unix_timestamp"].apply(
        lambda x: pd.Timestamp(x, unit="s", tz="Europe/Vilnius")
    )
    df = df[["timestamp", "open", "high", "low", "close", "volume"]]
    return df


pairs_df = fetch_pairs_from_db(psycopg_conn_str)
pairs_df["last_timestamp"] = pairs_df["pair_url"].apply(
    lambda pair_url: get_pair_latest_candle_timestamp(pair_url, psycopg_conn_str)
)

to_unix = lambda x: int(datetime.timestamp(x))


def get_new_ohlc(market_symbol, start):
    ohlc_list = []
    while start < curr_unix_time - 60:
        resp = requests.get(
            f"https://www.bitstamp.net/api/v2/ohlc/{market_symbol}",
            params={
                "step": 60,
                "limit": 1000,
                "start": start,
                "exclude_current_candle": True,
            },
        )
        results = resp.json()
        ohlc = results["data"]["ohlc"]
        for dict in ohlc:
            ohlc_list.append(dict)
        start = start + 60000
    return ohlc_list


def iterate_through_pairs_df_for_new_data():
    pairs_df = fetch_pairs_from_db(psycopg_conn_str)
    pairs_df["last_timestamp"] = pairs_df["pair_url"].apply(
        lambda x: get_pair_latest_candle_timestamp(x, psycopg_conn_str)
    )
    for row in pairs_df.iterrows():
        pair_url = row[1].values[1]
        start = row[1].values[2] + 60
        new_ohlc = get_new_ohlc(market_symbol=pair_url, start=start)
        new_ohlc_df = pd.DataFrame(new_ohlc)
        new_ohlc_df["timestamp"] = new_ohlc_df["timestamp"].apply(to_int)
        new_ohlc_df["high"] = new_ohlc_df["high"].apply(to_float)
        new_ohlc_df["open"] = new_ohlc_df["open"].apply(to_float)
        new_ohlc_df["low"] = new_ohlc_df["low"].apply(to_float)
        new_ohlc_df["close"] = new_ohlc_df["close"].apply(to_float)
        new_ohlc_df["volume"] = new_ohlc_df["volume"].apply(to_float)
        new_ohlc_df["timestamp"] = new_ohlc_df["timestamp"].apply(to_timestamp)
        new_ohlc_df["unique_pair_id"] = row[1].values[0]
        print(f"Updating database for: {row[1].values[0]} {pair_url}")
        new_ohlc_df.to_sql(
            f"ohlc_{pair_url}", sqlalchemy_conn_str, if_exists="append", index=False
        )


iterate_through_pairs_df_for_new_data()
