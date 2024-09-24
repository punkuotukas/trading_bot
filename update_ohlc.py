import os
import psycopg2
from psycopg2 import sql
import requests
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

url_dict = {
    "username": os.getenv("PSQL_USER"),
    "password": os.getenv("PSQL_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "database": os.getenv("DB_NAME"),
}

psycopg_conn_str = f"dbname={url_dict['database']} user={url_dict['username']} password={url_dict['password']} host={url_dict['host']} port={url_dict['port']}"


def fetch_pairs_from_db(connection):
    retrieve_pairs_query = """--sql
    SELECT pair_url FROM bitstamp_trading_pairs WHERE trading_enabled = TRUE;
    """
    with psycopg2.connect(connection) as conn:
        cur = conn.cursor()
        cur.execute(retrieve_pairs_query)
        results = cur.fetchall()
        cur.close()
    pairs_df = pd.DataFrame(results)
    pairs_df.rename(columns={0: "pair_url"}, inplace=True)
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
    return results


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
pairs_df["last_timestamp"] = pairs_df["last_timestamp"].apply(to_unix)
