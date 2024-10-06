import requests
import time
import datetime
import pytz
import psycopg2

cur_time = datetime.datetime.now(pytz.timezone("Europe/Vilnius")).replace(microsecond=0)
cur_unix_time = int(time.time())
four_week_interval = 60 * 60 * 24 * 7 * 4
one_week_interval = 60 * 60 * 24 * 7
one_day_interval = 60 * 60 * 24
one_hour_interval = 60 * 60
one_minute_interval = 60


class FindStartTimestamp:

    def get_candles(self, market_symbol, start, limit=1):
        ohlc_url = f"https://www.bitstamp.net/api/v2/ohlc/{market_symbol}/"
        params = {"step": 60, "limit": limit, "start": start}
        resp = requests.get(ohlc_url, params=params)
        results = resp.json()
        return results["data"]["ohlc"]

    def find_starting_timestamp(self, market_symbol, new_pair):
        if new_pair:
            start = cur_unix_time - one_week_interval
            results = self.get_candles(market_symbol, start)
            if not results:
                while not results:
                    start = start + one_day_interval
                    results = self.get_candles(market_symbol, start)
                    if results:
                        while results:
                            start = start - one_hour_interval
                            results = self.get_candles(market_symbol, start)
                            if not results:
                                while not results:
                                    start = start + one_minute_interval
                                    results = self.get_candles(market_symbol, start)
                                    if results:
                                        start_timestamp = results[0]["timestamp"]
                                        return start_timestamp
        elif not new_pair:
            start = cur_unix_time - four_week_interval
            results = self.get_candles(market_symbol, start)
            while results:  # * empty python list is considered falsy
                start = start - four_week_interval
                results = self.get_candles(market_symbol, start)
                if not results:
                    while not results:
                        start = start + one_week_interval
                        results = self.get_candles(market_symbol, start)
                        if results:
                            while results:
                                start = start - one_day_interval
                                results = self.get_candles(market_symbol, start)
                                if not results:
                                    while not results:
                                        start = start + one_hour_interval
                                        results = self.get_candles(market_symbol, start)
                                        if results:
                                            while results:
                                                start = start - one_minute_interval
                                                results = self.get_candles(
                                                    market_symbol, start
                                                )
                                                if not results:
                                                    while not results:
                                                        start = (
                                                            start + one_minute_interval
                                                        )
                                                        results = self.get_candles(
                                                            market_symbol, start
                                                        )
                                                        if results:
                                                            start_timestamp = results[
                                                                0
                                                            ]["timestamp"]
                                                            return start_timestamp

    def update_start_timestamp_in_main_table(
        self, timestamp, unix_timestamp, connection, pair
    ):
        update_start_timestamp_query = """--sql
        UPDATE bitstamp_pairs
        SET start_timestamp = %(timestamp)s,
            unix_timestamp = %(unix_timestamp)s
        WHERE
            pair_url = %(pair_url)s
        """
        with psycopg2.connect(connection) as conn:
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
