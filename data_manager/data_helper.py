"""
auxiliary functions to manipulate DB data
are contained in this module
"""
import os
import datetime
import time
from dotenv import load_dotenv
import psycopg
from psycopg import sql
import pytz
import pandas as pd
import numpy as np

load_dotenv()

API_PAIRS_URL = "https://www.bitstamp.net/api/v2/trading-pairs-info/"

class DataHelper:
    """
    Methods that contain specific SQL queries
    """
    def __init__(self) -> None:
        self.cur_time = datetime.datetime.now(pytz.timezone(
        "Europe/Vilnius")).replace(microsecond=0)
        self.cur_unix_time = int(time.mktime(self.cur_time.timetuple()))
        self.sql_dict = {
            "username": os.getenv("PSQL_USER"),
            "password": os.getenv("PSQL_PASSWORD"),
            "host": os.getenv("DB_HOST"),
            "port": os.getenv("DB_PORT"),
            "database": os.getenv("DB_NAME"),
        }
        self.psycopg_conn_str = f"""dbname={self.sql_dict['database']}
                            user={self.sql_dict['username']}
                            password={self.sql_dict['password']}
                            host={self.sql_dict['host']}
                            port={self.sql_dict['port']}"""
        self.sqlalchemy_conn_str = f"""postgresql+psycopg://{self.sql_dict['username']}:
                          {self.sql_dict['password']}@{self.sql_dict['host']}:
                          {self.sql_dict['port']}/{self.sql_dict['database']}"""


    def retrieve_df_with_last_candle(self, pair: str) -> pd.DataFrame:
        """
        auxialiary function to prepare a template dataframe
        which contains single row with the last existing ohlc data
        from DB for a specific pair
        """
        last_candle_query = sql.SQL("""--sql
        SELECT timestamp, open, high, low, close, volume
        FROM {}
        ORDER BY timestamp DESC
        LIMIT 1;
        """).format(sql.Identifier(f"ohlc_{pair}"))
        start_timestamp_query = sql.SQL("""--sql
        SELECT unix_timestamp
        FROM bitstamp_pairs
        WHERE pair_url = {}
        """).format(sql.Literal(pair))
        # pylint: disable=not-context-manager
        with psycopg.connect(self.psycopg_conn_str) as conn:
            cur = conn.cursor()
            cur.execute(last_candle_query)
            results = cur.fetchone()
            if results is not None:
                single_candle_df = pd.DataFrame(results).T
                single_candle_df.rename(columns={0: "unix_timestamp",
                                                 1: "open",
                                                 2: "high",
                                                 3: "low",
                                                 4: "close",
                                                 5: "volume"},
                                        inplace=True)
                single_candle_df["unix_timestamp"] = pd.to_datetime(
                    single_candle_df["unix_timestamp"]).dt.tz_convert("UTC").dt.floor("s")
                single_candle_df["unix_timestamp"] = single_candle_df[
                    "unix_timestamp"].astype("int64") // 10**9
                single_candle_df["unix_timestamp"] += 60
                return single_candle_df
            if results is None:
                cur.execute(start_timestamp_query)
                results = cur.fetchone()[0]
                single_candle_df = pd.DataFrame({"unix_timestamp": results,
                                                 "open": np.nan,
                                                 "high": np.nan,
                                                 "low": np.nan,
                                                 "close": np.nan,
                                                 "volume": np.nan},
                                                index=[0])
                return single_candle_df
            conn.commit()
            cur.close()
        raise ValueError(f"No data found for pair {pair}")

    @property
    def retrieve_trading_status_from_db(self) -> pd.DataFrame:
        """
        gets pair urls, trading status and last checked datetime from database and
        returns it as pandas dataframe
        """
        get_pairs_info_query = """--sql
        SELECT pair_url, trading_enabled, last_checked_for_trading
        FROM bitstamp_pairs;
        """
        # pylint:disable=not-context-manager
        with psycopg.connect(self.psycopg_conn_str) as conn:
            cur = conn.cursor()
            _ = cur.execute(get_pairs_info_query)
            results = cur.fetchall()
            cur.close()
        db_df = pd.DataFrame(results)
        db_df.rename(columns={0: 'pair_url',
                              1: 'trading_enabled',
                              2: 'last_checked_for_trading'},
                     inplace=True)
        return db_df


    @property
    def retrieve_traded_pairs_from_db(self) -> pd.DataFrame:
        """
        gets existing pairs from DB with trading_status = Enabled
        """
        retrieve_pairs_query = """--sql
        SELECT unique_pair_id,
               pair_url
        FROM bitstamp_pairs
        WHERE trading_enabled = TRUE;
        """
        # pylint:disable = not-context-manager
        with psycopg.connect(self.psycopg_conn_str) as conn:
            cur = conn.cursor()
            cur.execute(retrieve_pairs_query)
            results = cur.fetchall()
            cur.close()
        pairs_df = pd.DataFrame(results)
        pairs_df.rename(columns={0: "unique_pair_id", 1: "pair_url"}, inplace=True)
        return pairs_df


    def update_check_time(self, pair: str) -> None:
        """
        only check time for specific pair gets updated
        """
        update_check_time_query = """--sql
        UPDATE bitstamp_pairs
        SET last_checked_for_trading = %(cur_time)s
        WHERE pair_url = %(pair)s;
        """
        # pylint:disable = not-context-manager
        with psycopg.connect(self.psycopg_conn_str) as conn:
            cur = conn.cursor()
            cur.execute(
                update_check_time_query,
                {"cur_time": self.cur_time, "pair": pair},
            )
            conn.commit()


    def disable_trading_on_db(self, pair: str) -> None:
        """
        sets trading status to 'DISABLED' in DB if API doesn't return any ohlc data
        """
        update_trading_status_query = """--sql
            update bitstamp_pairs
            set trading_enabled = FALSE,
            where pair_url = %(pair)s;
            """
        # pylint:disable = not-context-manager
        with psycopg.connect(self.psycopg_conn_str) as conn:
            cur = conn.cursor()
            cur.execute(
                update_trading_status_query,
                {"pair": pair},
            )
            conn.commit()
            cur.close()
        self.update_check_time(pair)
        print(f"Trading status has been set to 'DISABLED' for: {pair}")


    def create_new_pair_table(self, conn, pair) -> None:
        """
        creates a new single pair table within DB
        """
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
        # pylint: disable = not-context-manager
        cur = conn.cursor()
        cur.execute(create_table_query)
        cur.execute(create_hypertable_query)
        print(f"New DB table created for {pair}")


    def insert_new_pairs_to_main_table(self, new_pairs: list[dict]) -> None:
        """
        In case during trading status for pairs check new pairs are detected,
        this function will insert them into bitstamp_pairs table
        """
        insert_new_pair_query = """--sql
        INSERT into bitstamp_pairs(pair,
                                   pair_url,
                                   trading_enabled,
                                   "description",
                                   minimum_order)
        VALUES (%(pair_name)s,
        %(pair_url)s,
        TRUE,
        %(description)s,
        %(minimum_order)s)
        """
        check_query = """--sql
        SELECT 1 FROM bitstamp_pairs WHERE pair_url = %(pair)s
        """
        # pylint:disable = not-context-manager
        with psycopg.connect(self.psycopg_conn_str) as conn:
            for pair in new_pairs:
                cur = conn.cursor()
                cur.execute(check_query, {"pair": pair["url_symbol"]})
                exists = cur.fetchone()
                if not exists:
                    try:
                        cur.execute(
                            insert_new_pair_query,
                            {
                                "pair_name": pair["name"],
                                "pair_url": pair["url_symbol"],
                                "description": pair["description"],
                                "minimum_order": pair["minimum_order"],
                            },
                        )
                        self.update_check_time(pair["url_symbol"])
                        self.create_new_pair_table(conn, pair["url_symbol"])
                        conn.commit()
                        cur.close()
                        print(
                            f"""pair {pair["name"]} has been added to bitstamp_pairs table""")
                    except psycopg.errors.UniqueViolation as e:
                        print(e)
                        print(f"pair {pair["name"]} already exists in the table")
                    finally:
                        cur.close()


    def insert_candles_to_db(self, df: pd.DataFrame, pair_url: str) -> None:
        """
        inserts given ohlc dataframe to DB
        """
        df.to_sql(
        f"ohlc_{pair_url}",
        self.sqlalchemy_conn_str,
        if_exists="append",
        index=False)


    @property
    def retrieve_pairs_without_start_timestamp(self) -> list[str]:
        """
        placeholder
        """
        pairs_without_timestamp: list[str] = []
        pairs_without_timestamp_query = """--sql
                SELECT
                pair_url
                FROM bitstamp_pairs
                WHERE
                start_timestamp IS NULL;
                """
        # pylint: disable=not-context-manager
        with psycopg.connect(self.psycopg_conn_str) as conn:
            cur = conn.cursor()
            cur.execute(pairs_without_timestamp_query)
            results = cur.fetchall()
            conn.commit()
            pairs_without_timestamp = [pair[0] for pair in results]
        return pairs_without_timestamp


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
        self.update_check_time(pair)
