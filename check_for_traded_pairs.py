import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv
import requests
import datetime
import time
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
cur_unix_time = int(time.mktime(cur_time.timetuple()))


# ? gets all pair urls from database and returns them as pandas dataframe with single column "pairs"
def retrieve_pair_urls_from_db():
    query_all_trading_pairs = """--sql
    SELECT pair_url FROM bitstamp_pairs;
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
    FROM bitstamp_pairs;
    """

    with psycopg2.connect(psycopg_conn_str) as conn:
        cur = conn.cursor()
        cur.execute(get_pairs_info_query)
        results = cur.fetchall()
        cur.close()
    db_df = pd.DataFrame(results)
    db_df.rename(columns={0: "pairs", 1: "status", 2: "last_checked"}, inplace=True)
    return db_df


# ? existing pairs in database are checked for trading status on api - if enabled on api and was enabled on db, update query for last check date to database is executed, if disabled on db and was enabled on api, update query for last check and trading status is executed. Same in case was disabled on db and enabled on api.
def update_db_pairs_status_based_on_api_data():
    status_updates = []
    disabled_pairs = []
    update_db_check_status_query = """--sql
    UPDATE bitstamp_pairs
    SET last_checked_for_trading = %(cur_time)s
    WHERE pair_url = %(pair)s;
    """
    update_db_check_and_trading_status_query = """--sql
    UPDATE bitstamp_pairs
    SET last_checked_for_trading = %(cur_time)s,
        trading_enabled = %(trading_status)s
    WHERE pair_url = %(pair)s;
    """
    df = retrieve_trading_status_from_db()
    enabled_db_pairs = df.loc[df["status"] == True, "pairs"].to_list()
    resp = requests.get(api_url)
    api_results = resp.json()
    api_pairs_list = [pair["url_symbol"] for pair in api_results if pair is not None]
    if len(api_pairs_list) > len(enabled_db_pairs):
        new_pairs_list = []
        print("New trading pairs have been launched")
        new_pairs = set(api_pairs_list) - set(enabled_db_pairs)
        print("New pairs are the following:")
        print(new_pairs)
        for pair in api_results:
            if pair["url_symbol"] in new_pairs:
                new_pairs_list.append(pair)
        insert_newly_traded_pairs_to_db(new_pairs_list)
        for pair in api_results:
            if pair["url_symbol"] in new_pairs:
                new_pairs_list.append(pair)
                pair_df_dict = pd.DataFrame(
                    {
                        "pairs": [pair["url_symbol"]],
                        "status": [pair["trading"]],
                        "last_checked": [cur_time],
                    }
                )
                df = pd.concat([df, pair_df_dict], ignore_index=True)
                create_new_pair_table(psycopg_conn_str, pair["url_symbol"])
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
            print(f"Trading status updated for: {pair["name"]}")
            status_updates.append(pair["name"])
    [disabled_pairs.append(pair) for pair in df["pairs"] if pair not in api_pairs_list]
    for pair in disabled_pairs[:]:
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


# ? In case during trading status for pairs check new pairs are detected, this function will insert them into bitstamp_pairs table
def insert_newly_traded_pairs_to_db(new_pairs):
    insert_new_pair_query = """--sql
    INSERT into bitstamp_pairs(pair, start_timestamp, unix_timestamp, pair_url, trading_enabled, "description", minimum_order)
    VALUES (%(pair_name)s, %(start_timestamp)s, %(unix_timestamp)s, %(pair_url)s, TRUE, %(description)s, %(minimum_order)s)
    """
    for pair in new_pairs:
        pair["start_timestamp"] = cur_time
        pair["unix_timestamp"] = cur_unix_time
        with psycopg2.connect(psycopg_conn_str) as conn:
            cur = conn.cursor()
            cur.execute(
                insert_new_pair_query,
                {
                    "pair_name": pair["name"],
                    "start_timestamp": pair["start_timestamp"],
                    "unix_timestamp": pair["unix_timestamp"],
                    "pair_url": pair["url_symbol"],
                    "description": pair["description"],
                    "minimum_order": pair["minimum_order"],
                },
            )
            conn.commit()
            cur.close()
        print(f"pair {pair["name"]} has been added to database table")


def create_new_pair_table(connection, pair):
    table_name = f"ohlc_{pair}"
    create_table_query = f"""--sql
    CREATE TABLE IF NOT EXISTS {table_name} (
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
    DO $$
    BEGIN
        IF NOT EXISTS (
        SELECT 1
        FROM timescaledb_information.hypertables
        WHERE hypertable_name = 'ohlc_{pair}'
        ) THEN
            PERFORM create_hypertable('ohlc_{pair}', by_range('timestamp'));
        END IF;
    END $$;
    """
    with psycopg2.connect(connection) as conn:
        cur = conn.cursor()
        cur.execute(create_table_query)
        cur.execute(create_hypertable_query)
        conn.commit()
        print(f"New DB table created for {pair}")
        cur.close()


def check_pairs_status():
    disabled_pairs = update_db_pairs_status_based_on_api_data()
    update_trading_status_query = """--sql
        update bitstamp_pairs
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
        print(f"Trading status has been set to 'DISABLED' for: {disabled_pairs[0]}")
    elif len(disabled_pairs) > 1:
        for i in range(len(disabled_pairs)):
            with psycopg2.connect(psycopg_conn_str) as conn:
                cur = conn.cursor()
                cur.execute(
                    update_trading_status_query,
                    {"pair": disabled_pairs[0], "cur_time": cur_time},
                )
                conn.commit()
                cur.close()
            print(f"Trading status has been set to 'DISABLED' for: {disabled_pairs[0]}")
            del disabled_pairs[0]


check_pairs_status()
