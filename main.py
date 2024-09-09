import os
import psycopg
import requests
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine

load_dotenv()
curr_time = datetime.timestamp(datetime.now())

url_dict = {
    "username": os.getenv("PSQL_USER"),
    "password": os.getenv("PSQL_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "database": os.getenv("DB_NAME"),
}

sqlalchemy_conn_str = f"postgresql+psycopg://{url_dict['username']}:{url_dict['password']}@{url_dict['host']}:{url_dict['port']}/{url_dict['database']}"
psycopg_conn_str = f"dbname={url_dict['database']} user={url_dict['username']} password={url_dict['password']} host={url_dict['host']} port={url_dict['port']}"


def fetch_trading_pairs(connection):
    with psycopg.connect(connection) as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT unique_pair_id, pair_url, unix_timestamp FROM bitstamp_trading_pairs;"
        )
        results = cur.fetchall()
        cur.close()
    return results


def create_trading_pair_table(connection, pair):
    table_name = f"ohlc_{pair}"
    create_table_query = f"""--sql
    CREATE TABLE {table_name} (
    unique_pair_id INT,
    "timestamp" TIMESTAMPTZ not null,
    "open" NUMERIC(20, 12),
    high NUMERIC(20, 12),
    low NUMERIC(20, 12),
    "close" NUMERIC(20, 12),
    volume NUMERIC(28, 12)
    );
    """
    create_hypertable_query = f"""--sql
    SELECT create_hypertable('ohlc_{pair}', by_range('timestamp'))
    """
    with psycopg.connect(connection) as conn:
        cur = conn.cursor()
        cur.execute(create_table_query)
        cur.execute(create_hypertable_query)
        conn.commit()
        cur.close()


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


def pass_data_to_db(sql_connection):
    base_df = pd.DataFrame(
        fetch_trading_pairs(sql_connection),
        columns=[
            "unique_pair_id",
            "pair_url",
            "unix_timestamp",
        ],
    )
    for pair in base_df[::-1].values:
        pair_df = pd.DataFrame(
            columns=["timestamp", "open", "high", "low", "close", "volume"]
        )
        pair_start_time = pair[2]
        pair_url = pair[1]
        while pair_start_time < curr_time - 60:
            pair_df = pd.concat(
                [pair_df, (fetch_bitstamp_ohlc(pair_url, pair_start_time))], axis=0
            )
            pair_start_time += 60000
        pair_df["unique_pair_id"] = base_df.loc[
            (base_df["pair_url"] == pair_url), "unique_pair_id"
        ].values[0]
        pair_df = pair_df[
            ["unique_pair_id", "timestamp", "open", "high", "low", "close", "volume"]
        ]
        pair_df["open"] = pair_df["open"].astype("float")
        pair_df["high"] = pair_df["high"].astype("float64")
        pair_df["low"] = pair_df["low"].astype("float64")
        pair_df["close"] = pair_df["close"].astype("float64")
        pair_df["volume"] = pair_df["volume"].astype("float64")
        create_trading_pair_table(sql_connection, pair_url)
        db = create_engine(url=sqlalchemy_conn_str)
        conn = db.connect()
        pair_df.to_sql(f"ohlc_{pair_url}", conn, if_exists="append", index=False)


pass_data_to_db(psycopg_conn_str)
