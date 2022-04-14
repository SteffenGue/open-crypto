#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This module contains scripts to demonstrate the features of the application.

Classes:
 - Examples: Contains examples and illustrations to demonstrate all request methods.
"""
import datetime
import os
import pathlib
from typing import Optional

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.pyplot import GridSpec
from sqlalchemy import func, desc
from sqlalchemy.orm import Session

# noinspection PyUnresolvedReferences
import _paths  # pylint: disable=unused-import
from main import run as main_run
from model.database.tables import *
from model.utilities.export import database_session as get_session
from model.utilities.kill_switch import KillSwitch
from model.utilities.settings import Settings


class Examples:
    """
    Helper class providing examples and illustrations for all request methods.

    The respective configuration files are named according to the class-methods and can be found in the
    resources/configs folder. All requests are configured to terminate after a single run.
    """
    configuration_file: str
    plt.style.use("ggplot")
    pd.set_option("display.max_columns", 99)
    pd.set_option("expand_frame_repr", False)

    PATH = pathlib.Path.joinpath(_paths.all_paths.get("path_absolut"), "resources")

    @staticmethod
    def __start_catch_systemexit(configuration_file: str) -> None:
        try:
            main_run(configuration_file, os.getcwd())
        except SystemExit:
            return

    @staticmethod
    def __clear_database_table(session: Session, table: DatabaseTableType) -> None:
        """
        Deletes all entries from a database table.
        @param session: SQLAlchemy-ORM Session.
        @param table: Database table
        """
        print(f"Clearing table: {table.__name__}.")
        session.query(table).delete()
        session.commit()

    @staticmethod
    def __check_resources() -> Optional[bool]:
        """
        Check if the resources are available in the current working directory.
        """
        if not os.path.exists(Examples.PATH):
            print("Copy resources to your working directory first. "
                  "Use: runner.update_maps().")
            return False

    @staticmethod
    def static() -> plt.hist:
        """
        Request all available exchanges currency-pairs and create a histogram of their distribution.
        """
        if Examples.__check_resources() is False:
            return

        print("\nWarning: This example takes several minutes to complete. Do not interrupt the data requesting.")
        configuration_file = "examples/static"
        session = get_session(configuration_file)

        Examples.__start_catch_systemexit(configuration_file)

        query = session.query(ExchangeCurrencyPairView)
        dataframe = pd.read_sql(query.statement, con=session.bind)
        if dataframe.empty:
            return
        dataframe.exchange_name.value_counts().hist(bins=len(set(dataframe.exchange_name)))
        plt.title("Traded Pairs on Exchanges")
        plt.ylabel("Number of Exchanges")
        plt.xlabel("Number of Traded Pairs")
        plt.tight_layout()
        plt.show()

    @staticmethod
    def platforms() -> plt.plot:
        """
        Request BTC-USD data from the platform 'www.coingecko.com' and create a plot.
        """
        if Examples.__check_resources() is False:
            return

        configuration_file = "examples/platform"

        session = get_session(configuration_file)
        Examples.__start_catch_systemexit(configuration_file)

        query = session.query(HistoricRateView).filter(HistoricRateView.exchange == "COINGECKO",
                                                       HistoricRateView.first_currency == "BITCOIN",
                                                       HistoricRateView.second_currency == "USD")
        dataframe = pd.read_sql(query.statement, con=session.bind, index_col="time")
        if dataframe.empty:
            return
        dataframe.sort_index(inplace=True)

        fig = plt.figure(constrained_layout=True, figsize=(8, 6))
        grid_spec = GridSpec(4, 4, figure=fig)
        plt.rc("grid", linestyle=":", color="black")

        ax0 = fig.add_subplot(grid_spec[0:2, :])
        ax0.plot(dataframe.close, label="Close")
        plt.setp(ax0.get_xticklabels(), visible=False)
        plt.title("Bitcoin Daily Close in US-Dollar")
        ax0.grid(True)

        ax1 = fig.add_subplot(grid_spec[2:3, :])
        ax1.bar(dataframe.volume[dataframe.volume < 150 * 1e9].index,
                dataframe.volume[dataframe.volume < 150 * 1e9] / 1e9, label="Volume")
        plt.setp(ax1.get_xticklabels(), visible=False)
        ax1.grid(True)
        ax1.set_ylabel("Billion")
        plt.title("Bitcoin Daily Volume in US-Dollar")

        ax2 = fig.add_subplot(grid_spec[3:4, :])
        ax2.plot((dataframe.market_cap.divide(dataframe.close, axis=0) / 1e6), label="Supply")
        ax2.grid(True)
        ax2.set_ylabel("Million")
        ax2.set_xlabel("Time (Daily)")
        plt.title("Bitcoin Total Coin Supply")
        plt.tight_layout()
        plt.show()

    @staticmethod
    def minute_candles(timer: int = 60) -> plt.plot:
        """
        Request BTC-USD(T) data from several exchanges and plot them simultaneously.
        """
        if Examples.__check_resources() is False:
            return

        print(f"Note: The program will run for {timer} seconds before terminating automatically.")
        configuration_file = "examples/minute_candles"
        session = get_session(configuration_file)
        Examples.__clear_database_table(session, HistoricRate)

        switch: KillSwitch
        with KillSwitch() as switch:
            switch.set_timer(timer)
            Examples.__start_catch_systemexit(configuration_file)

        exchanges = ("BINANCE", "BITTREX", "HITBTC")
        session = get_session(configuration_file)
        query = session.query(HistoricRateView)
        query = query.filter(HistoricRateView.exchange.in_(exchanges))

        dataframe = pd.read_sql(query.statement, con=session.bind, index_col="time")
        if dataframe.empty:
            return
        dataframe = pd.pivot_table(dataframe, columns=dataframe.exchange, index=dataframe.index)
        dataframe = dataframe.close

        for column in dataframe.columns:
            plt.plot(dataframe.loc[:, column].dropna(), linestyle="dotted", linewidth=.75, label=column)
        plt.title("ETH/BTC - Minute Candles")
        plt.xlabel("Time")
        plt.ylabel("Price in US-Dollar")
        plt.legend()
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()

    @staticmethod
    def trades() -> Optional[pd.DataFrame]:
        """
        Request ETH-BTC transaction data from Coinbase and plot the price series and trade direction.
        """
        if Examples.__check_resources() is False:
            return

        configuration_file = "examples/trades"

        Examples.__start_catch_systemexit(configuration_file)

        exchange = "COINBASE"
        session = get_session(configuration_file)
        query = session.query(TradeView).filter(TradeView.exchange == exchange). \
            order_by(desc(TradeView.time)).limit(1000)

        dataframe = pd.read_sql(query.statement, con=session.bind, index_col="time")
        if dataframe.empty:
            return
        dataframe.sort_index(inplace=True)

        plt.plot(dataframe[dataframe.direction == "sell"].loc[:, "price"], linestyle="dotted",
                 color="red", label="Sells", linewidth=1.5)
        plt.plot(dataframe[dataframe.direction == "buy"].loc[:, "price"], linestyle="dotted",
                 color="green", label="Buys", linewidth=1.5)

        plt.xlabel("Timestamp")
        plt.xticks(rotation=45)
        plt.legend()
        plt.title("ETH/BTC Trades from Coinbase")

        plt.ylabel("Price in BTC")
        plt.tight_layout()
        plt.show()
        return dataframe.head(10)

    @staticmethod
    def order_books() -> Optional[pd.DataFrame]:
        """
        Requests the current order-book snapshot from Coinbase and plot the market depth.
        """
        if Examples.__check_resources() is False:
            return

        configuration_file = "examples/order_books"
        exchange = "COINBASE"
        session = get_session(configuration_file)
        Examples.__start_catch_systemexit(configuration_file)

        (timestamp,) = session.query(func.max(OrderBookView.time)).first()
        query = session.query(OrderBookView).filter(OrderBookView.exchange == exchange,
                                                    OrderBookView.time == timestamp,
                                                    OrderBookView.position <= 50)

        dataframe = pd.read_sql(query.statement, con=session.bind, index_col="time")
        if dataframe.empty:
            return

        # insert row that plot starts at amount = 0.
        timestamp = dataframe.index[0] - datetime.timedelta(days=1)
        new_data = dataframe.iloc[0, :].to_dict()
        template = {timestamp: {"bids_price": None, "bids_amount": 0, "asks_price": None,
                                "asks_amount": 0, "position": -1}}
        template.get(timestamp).update((k, new_data[k]) for k in set(new_data).intersection(template.get(timestamp))
                                       if template.get(timestamp).get(k) is None)

        dataframe = pd.concat([dataframe, pd.DataFrame.from_dict(template, orient="index")], axis=0)
        dataframe.sort_values(by="position", ascending=True, inplace=True)

        plt.step(dataframe.bids_price, dataframe.bids_amount.cumsum(), color="green", label="bids")
        plt.step(dataframe.asks_price, dataframe.asks_amount.cumsum(), color="red", label="asks")

        plt.ylim(ymin=0)
        plt.title("Market Depth BTC/USD(T)")
        plt.xlabel("Price in USD(T)")
        plt.ylabel("Accum. Size in BTC")
        plt.legend()
        plt.tight_layout()
        plt.show()
        return dataframe.iloc[1:10, :]

    @staticmethod
    def exchange_listings() -> plt.plot:
        """
        Collects historical data for 10 currency-pairs quoted against USD(T) and plots the amount of exchanges,
        each currency was listed on over time.
        """
        if Examples.__check_resources() is False:
            return

        print("\nWarning: This example takes several minutes to complete. Do not interrupt the data requesting.")
        configuration_file = "examples/exchange_listings"

        settings: Settings
        with Settings() as settings:
            settings.set("request_settings", "min_return_tuples", 100)
            settings.set("request_settings", "interval_settings", "equal")

            Examples.__start_catch_systemexit(configuration_file)

        session = get_session(configuration_file)
        base_currencies = ("BTC", "LINK", "ETH", "XRP", "LTC", "ATOM", "ADA", "XLM", "BCH", "DOGE")
        query = session.query(HistoricRateView.time,
                              HistoricRateView.exchange,
                              HistoricRateView.first_currency,
                              HistoricRateView.close).filter(HistoricRateView.first_currency.in_(base_currencies))
        dataframe = pd.read_sql(query.statement, con=session.bind, index_col="time")
        if dataframe.empty:
            return
        dataframe = pd.pivot_table(dataframe, columns=[dataframe.exchange, dataframe.first_currency],
                                   index=dataframe.index).close["2010-01-01":]

        for currency in base_currencies:
            temp = dataframe.loc[:, (slice(None), currency.upper())]
            temp = temp.resample("d").mean()
            temp = temp.resample("m").median()
            temp.count(axis=1).plot(label="/".join([currency, "USD(T)"]))

        plt.legend()
        plt.xlabel("Time (Monthly)")
        plt.ylabel("Number of Exchanges")
        plt.tight_layout()
        plt.show()
