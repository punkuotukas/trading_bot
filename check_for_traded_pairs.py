import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv
import requests
import datetime
import pytz

load_dotenv()

api_url = "https://www.bitstamp.net/api/v2/trading-pairs-info/"
sql_dict = {
    "username": os.getenv("PSQL_USER"),
    "password": os.getenv("PSQL_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "database": os.getenv("DB_NAME"),
}
psycopg_conn_str = f"dbname={sql_dict['database']} user={sql_dict['username']} password={sql_dict['password']} host={sql_dict['host']} port={sql_dict['port']}"

cur_time = datetime.datetime.now(pytz.timezone("Europe/Vilnius")).replace(microsecond=0)


def retrieve_pair_urls_from_db():
    query_all_trading_pairs = """--sql
    SELECT pair_url FROM bitstamp_trading_pairs;
    """

    with psycopg2.connect(psycopg_conn_str) as conn:
        cur = conn.cursor()
        cur.execute(query_all_trading_pairs)
        results = cur.fetchall()
        cur.close()
    db_df = pd.DataFrame(results)
    db_df.rename(columns={0: "pairs"}, inplace=True)
    return db_df


def validate_api_data():
    update_db_check_status_query = """--sql
    update bitstamp_trading_pairs
    set last_checked_for_trading = %(cur_time)s
    where pair_url = %(pair)s;
    """
    df = retrieve_pair_urls_from_db()
    resp = requests.get(api_url)
    api_results = resp.json()
    api_pairs_list = [pair["url_symbol"] for pair in api_results if pair is not None]
    for pair in api_results:
        if pair["trading"] == "Enabled":
            with psycopg2.connect(psycopg_conn_str) as conn:
                cur = conn.cursor()
                cur.execute(
                    update_db_check_status_query,
                    {"cur_time": cur_time, "pair": pair["url_symbol"]},
                )
                conn.commit()
    disabled_pairs = []
    [disabled_pairs.append(pair) for pair in df["pairs"] if pair not in api_pairs_list]
    return disabled_pairs


def check_pairs_status():
    disabled_pairs = validate_api_data()
    update_trading_status_query = """--sql
        update bitstamp_trading_pairs
        set trading_enabled = FALSE,
        last_checked_for_trading = %(cur_time)s
        where pair_url = %(pair)s;
        """
    if len(disabled_pairs) == 1:
        with psycopg2.connect(psycopg_conn_str) as conn:
            cur = conn.cursor()
            cur.execute(
                update_trading_status_query,
                {"pair": disabled_pairs[0], "cur_time": cur_time},
            )
            conn.commit()
            cur.close()
    elif len(disabled_pairs) > 1:
        for i in range(len(disabled_pairs)):
            with psycopg2.connect(psycopg_conn_str) as conn:
                cur = conn.cursor()
                cur.execute(
                    update_trading_status_query,
                    {"pair": disabled_pairs[0], "cur_time": cur_time},
                )
                cur.close()
            del disabled_pairs[0]


check_pairs_status()
