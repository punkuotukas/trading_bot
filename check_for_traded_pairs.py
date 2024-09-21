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


# ? gets all pair urls from database and returns them as pandas dataframe with single column "pairs"
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


# ? gets pair urls, trading status and last checked datetime from database and returns it as pandas dataframe
def retrieve_trading_status_from_db():
    get_pairs_info_query = """--sql
    SELECT pair_url, trading_enabled, last_checked_for_trading
    FROM bitstamp_trading_pairs;
    """

    with psycopg2.connect(psycopg_conn_str) as conn:
        cur = conn.cursor()
        cur.execute(get_pairs_info_query)
        results = cur.fetchall()
        cur.close()
    db_df = pd.DataFrame(results)
    db_df.rename(columns={0: "pairs", 1: "status", 2: "last_checked"}, inplace=True)
    return db_df


# ? existing pairs in database are checked for trading status on api - if enabled on api and was enabled on db, update query for last check date to database is executed, if disabled on api and was enabled on api, update query for last check and trading status is executed. Same in case was disabled on db and enabled on api.
def update_db_pairs_status_based_on_api_data():
    status_updates = []
    disabled_pairs = []
    update_db_check_status_query = """--sql
    UPDATE bitstamp_trading_pairs
    SET last_checked_for_trading = %(cur_time)s
    WHERE pair_url = %(pair)s;
    """
    update_db_check_and_trading_status_query = """--sql
    UPDATE bitstamp_trading_pairs
    SET last_checked_for_trading = %(cur_time)s,
        trading_enabled = %(trading_status)s
    WHERE pair_url = %(pair)s;
    """
    df = retrieve_trading_status_from_db()
    resp = requests.get(api_url)
    api_results = resp.json()
    api_pairs_list = [pair["url_symbol"] for pair in api_results if pair is not None]
    for pair in api_results:
        df_pair_status = df.loc[df["pairs"] == pair["url_symbol"], "status"].values[0]
        if pair["trading"] == "Enabled" and df_pair_status == True:
            with psycopg2.connect(psycopg_conn_str) as conn:
                cur = conn.cursor()
                cur.execute(
                    update_db_check_status_query,
                    {"cur_time": cur_time, "pair": pair["url_symbol"]},
                )
                conn.commit()
        elif pair["trading"] == "Enabled" and df_pair_status == False:
            with psycopg2.connect(psycopg_conn_str) as conn:
                cur = conn.cursor()
                cur.execute(
                    update_db_check_and_trading_status_query,
                    {
                        "cur_time": cur_time,
                        "pair": pair["url_symbol"],
                        "trading_status": df_pair_status,
                    },
                )
                conn.commit()
            status_updates.append(pair["name"])
    [disabled_pairs.append(pair) for pair in df["pairs"] if pair not in api_pairs_list]
    for pair in disabled_pairs:
        if df.loc[df["pairs"] == pair, "status"].values[0] == False:
            disabled_pairs.remove(pair)
    if len(disabled_pairs) > 0 or len(status_updates) > 0:
        print("THERE WERE CHANGES IN TRADING STATUS")
        print("Disabled pairs:")
        print(disabled_pairs)
        print("Enabled pairs:")
        print(status_updates)
    else:
        print("No changes in trading status since last check.")
    return disabled_pairs


def check_pairs_status():
    disabled_pairs = update_db_pairs_status_based_on_api_data()
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


update_db_pairs_status_based_on_api_data()
