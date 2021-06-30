#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This module provides basic examples about the cryptocurrency data-collector. For each request-method a function
is provided to download and plot the data, in accordance with the publication paper.
"""
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import func

import runner


class Examples:
    """
    Helper class imported by the runner-module providing all methods for illustrations.
    """

    plt.style.use('ggplot')

    @staticmethod
    def platform() -> plt.plot:
        """
        Request Bitcoin - USD data from the platform 'www.coingecko.com' and create a plot.
        The configuration file called "platform.yaml" can be found within the resources folder.
        """
        configuration_file = 'config'

        # Execute the runner and catch the SystemExit exception to keep the process running.
        try:
            runner.run(configuration_file)
        except SystemExit:
            pass

        session = runner.get_session(configuration_file)
        query = session.query(runner.HistoricRate)
        dataframe = pd.read_sql(query.statement, con=session.bind, index_col='time')

        plt.plot(dataframe.close, label="Bitcoin/USD")
        plt.legend()
        plt.show()

    @staticmethod
    def historic_rates() -> plt.plot:
        """
        Request BTC-USD(T) data from several exchanges and plot them simultaneously.
        """
        configuration_file = 'historic'

        try:
            runner.run(configuration_file)
        except SystemExit:
            pass

        exchanges = ('BITFINEX', 'BINANCE', 'COINBASE')
        session = runner.get_session(configuration_file)
        query = session.query(runner.HistoricRateView)
        query = query.filter(runner.HistoricRateView.exchange.in_(exchanges))

        dataframe = pd.read_sql(query.statement, con=session.bind, index_col='time')
        dataframe = pd.pivot_table(dataframe, columns=dataframe.exchange, index=dataframe.index)
        dataframe = dataframe.resample("d").last()


        plt.plot(dataframe.close, linestyle="dotted", label=dataframe.close.columns)
        plt.title("ETH/BTC - Daily Candles")
        plt.legend()
        plt.show()

    @staticmethod
    def trades() -> plt.plot:
        """
        Request ETH/BTC transaction data from Coinbase and plot the price series.
        """
        configuration_file = 'trades'

        try:
            runner.run(configuration_file)
        except SystemExit:
            pass

        exchange = "COINBASE"
        session = runner.get_session(configuration_file)
        query = session.query(runner.TradeView).filter(runner.TradeView.exchange == exchange)

        dataframe = pd.read_sql(query.statement, con=session.bind, index_col='time')
        dataframe.sort_index(inplace=True)

        plt.plot(dataframe[dataframe.direction == 0].loc[:, "price"], linestyle="dotted",
                 color="red", label="Sells", linewidth=1.5)
        plt.plot(dataframe[dataframe.direction == 1].loc[:, "price"], linestyle="dotted",
                 color="green", label="Buys", linewidth=1.5)

        plt.xticks(rotation=45)
        plt.legend()
        plt.title("ETH/BTC Trades from Coinbase")
        plt.tight_layout()
        plt.show()

    @staticmethod
    def order_books() -> plt.plot:
        """
        Requests the current order-book snapshot from Coinbase and plot the market depth.
        """
        configuration_file = 'order_books'
        exchange = 'COINBASE'
        session = runner.get_session(configuration_file)
        try:
            runner.run(configuration_file)
        except SystemExit:
            pass

        (timestamp,) = session.query(func.max(runner.OrderBookView.time)).first()
        query = session.query(runner.OrderBookView).filter(runner.OrderBookView.exchange == exchange,
                                                           runner.OrderBookView.time == timestamp)
        dataframe = pd.read_sql(query.statement, con = session.bind, index_col='time')

        bids = dataframe.groupby(pd.cut(dataframe.bids_price, bins=10))['bids_amount'].sum()\
            .sort_index(ascending=False).cumsum()
        asks = dataframe.groupby(pd.cut(dataframe.asks_price, bins=10))['asks_amount'].sum()\
            .sort_index(ascending=True).cumsum()

        index1 = [item.right.__round__() for item in bids.index]
        index2 = [item.left.__round__() for item in asks.index]

        plt.step(index1, bids.values, color='green', label="Bids")
        plt.step(index2, asks.values, color="red", label="Asks")
        plt.title("Market Depth BTC/USD(T)")
        plt.xlabel("Price in USD(T)")
        plt.ylabel("Size in BTC")
        plt.legend()
        plt.tight_layout()
        plt.show()











