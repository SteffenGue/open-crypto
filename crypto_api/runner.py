#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This module is a wrapper around the whole package. Its main function is to export all relevant configuration files
to the current working directory of the user, start the program, establish database connections and export data
into csv-files.
"""
import _paths
import os
import shutil
from typing import Any, Optional

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.pyplot import GridSpec
from sqlalchemy import func
from sqlalchemy.orm.session import Session

import main
from export import CsvExport, database_session
from model.database.tables import *
from model.utilities.utilities import read_config, get_all_exchanges_and_methods, prepend_spaces_to_columns


def check_path(path: str) -> None:
    """
    Checks if all resources are in the current working directory. If not, calls the function update_maps()
    """
    destination = path + "/resources"
    if not os.path.exists(destination):
        update_maps(path)


def update_maps(directory: str) -> None:
    """
    Copies everything from the folder "resources" into the current working directory. If files already exist,
    the method will override them (i.e. first delete and then copy).
    @type directory: Current working directory
    """

    print(f"Copying resources to {directory} ...")
    source = os.path.dirname(os.path.realpath(__file__)) + "/resources"

    destination = directory + "/resources"
    for src_dir, dirs, files in os.walk(source):
        dst_dir = src_dir.replace(source, destination, 1)
        try:
            dirs.remove('templates')
            dirs.remove('__pycache__')
        except ValueError:
            pass

        if not os.path.exists(dst_dir):
            os.makedirs(dst_dir)
        for file_ in files:
            src_file = os.path.join(src_dir, file_)
            dst_file = os.path.join(dst_dir, file_)
            if os.path.exists(dst_file):
                # in case of the src and dst are the same file
                if os.path.samefile(src_file, dst_file):
                    continue
                os.remove(dst_file)
            if not src_file.endswith('.py'):
                shutil.copy(src_file, dst_dir)


def get_session(filename: str, db_path: str = os.getcwd()) -> Session:
    """
    Returns an open SqlAlchemy-Session. The session is obtained from the DatabaseHandler via the module export.py.
    Furthermore this functions imports all database defining classes to work with.

    @param db_path: path to the database. Default: current working directory
    @param filename: Name of the configuration file to init the DatabaseHandler
    @return: SqlAlchemy-Session
    """
    return database_session(filename=filename, db_path=db_path)


def exchanges_and_methods(return_dataframe: bool = False) -> pd.DataFrame:
    """
    Lists all exchanges and methods.
    as_dataframe: Specify if results are returned as pd.DataFrame.
    @type return_dataframe: boolean
    @return: Print or return dataframe
    @rtype: None or pd.DataFrame
    """
    dataframe = pd.DataFrame.from_dict(get_all_exchanges_and_methods())
    pd.set_option('display.max_rows', 500)

    if return_dataframe:
        return dataframe.transpose()
    else:
        print(prepend_spaces_to_columns(dataframe.transpose(), 3))


def get_config(filename: str = None) -> dict[str, Any]:
    """
    Returns the actual config-file.
    @param filename: name of the config file.
    @return: Returns the current config.
    """
    return read_config(file=filename)


def get_config_template(csv: bool = False) -> None:
    """
    Creates a copy of the config templates and puts it into the resources/configs folder.
    @param csv: boolean: If True, create an csv-export config. Else create a config for the runner.
    """
    if csv:
        filename = "csv_config_template.yaml"
    else:
        filename = "config_template.yaml"

    source = os.path.dirname(os.path.realpath(__file__)) + "/resources/templates"
    destination = os.getcwd() + "/resources/configs"

    if os.path.exists(os.path.join(destination, filename)):
        os.remove(os.path.join(destination, filename))

    shutil.copy(os.path.join(source, filename),
                os.path.join(destination, filename))
    print("Created new config template.")


def export(file: Optional[str] = None, data_type: str = 'csv', *args: Any, **kwargs: Any) -> None:
    """
    Calls the imported module CsvExport and the respective method create_csv(). This will take a csv-export config as
    input and write data into a csv-file depending on the configuration.
    @param data_type:
    @param file: Name of the csv-export configuration file.
    """
    CsvExport(file).export(data_type=data_type, *args, **kwargs)


def run(configuration_file: Optional[str] = None) -> None:
    """
    First checks if all necessary folder are available (i.e. config and yaml-maps) and starts the program.
    """
    # configuration_file = None
    working_directory = os.getcwd()

    check_path(working_directory)
    main.run(configuration_file, working_directory)


class Examples:
    """
    Helper class providing illustrations for all request methods. The respective configuration files are
    named according to the class-methods and can be found in the resources/configs folder. All requests
    are configured to terminate after a single run.
    """

    configuration_file: str
    plt.style.use('ggplot')
    pd.set_option('display.max_columns', None)

    @classmethod
    def __start_with_except(cls, configuration_file: str) -> None:
        try:
            run(configuration_file)
        except SystemExit:
            return

    @staticmethod
    def static() -> plt.hist:
        """
        Request all available exchanges currency-pairs and create a histogram of their distribution.
        """
        configuration_file = 'static'
        session = get_session(configuration_file)

        Examples.__start_with_except(configuration_file)

        query = session.query(ExchangeCurrencyPairView)
        dataframe = pd.read_sql(query.statement, con=session.bind)

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
        configuration_file = 'platform'

        Examples.__start_with_except(configuration_file)

        session = get_session(configuration_file)
        query = session.query(HistoricRate)
        dataframe = pd.read_sql(query.statement, con=session.bind, index_col='time')
        dataframe.sort_index(inplace=True)

        fig = plt.figure(constrained_layout=True, figsize=(8, 6))
        grid_spec = GridSpec(4, 4, figure=fig)
        plt.rc('grid', linestyle=":", color='black')

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
        ax2.plot((dataframe.market_cap / dataframe.close) / 1e6, label="Supply")
        ax2.grid(True)
        ax2.set_ylabel("Million")
        ax2.set_xlabel("Time (Daily)")
        plt.title("Bitcoin Total Coin Supply")
        plt.show()

    @staticmethod
    def historic_rates() -> plt.plot:
        """
        Request BTC-USD(T) data from several exchanges and plot them simultaneously.
        """
        configuration_file = 'historic'

        Examples.__start_with_except(configuration_file)

        exchanges = ('BITFINEX', 'BINANCE', 'COINBASE')
        session = get_session(configuration_file)
        query = session.query(HistoricRateView)
        query = query.filter(HistoricRateView.exchange.in_(exchanges))

        dataframe = pd.read_sql(query.statement, con=session.bind, index_col='time')
        dataframe = pd.pivot_table(dataframe, columns=dataframe.exchange, index=dataframe.index)
        dataframe = dataframe.resample("d").last()

        plt.plot(dataframe.close, linestyle="dotted", label=dataframe.close.columns)
        plt.title("ETH/BTC - Daily Candles")
        plt.xlabel("Time in Days")
        plt.ylabel("Price in US-Dollar")
        plt.legend()
        plt.show()

    @staticmethod
    def trades() -> plt.plot:
        """
        Request ETH-BTC transaction data from Coinbase and plot the price series and trade direction.
        """
        configuration_file = 'trades'

        Examples.__start_with_except(configuration_file)

        exchange = "COINBASE"
        session = get_session(configuration_file)
        query = session.query(TradeView).filter(TradeView.exchange == exchange)

        dataframe = pd.read_sql(query.statement, con=session.bind, index_col='time')
        dataframe.sort_index(inplace=True)

        plt.plot(dataframe[dataframe.direction == 0].loc[:, "price"], linestyle="dotted",
                 color="red", label="Sells", linewidth=1.5)
        plt.plot(dataframe[dataframe.direction == 1].loc[:, "price"], linestyle="dotted",
                 color="green", label="Buys", linewidth=1.5)

        plt.xticks(rotation=45)
        plt.legend()
        plt.title("ETH/BTC Trades from Coinbase")
        plt.xlabel("Timestamp")
        plt.ylabel("Price in BTC")
        plt.tight_layout()
        plt.show()

    @staticmethod
    def order_books() -> plt.plot:
        """
        Requests the current order-book snapshot from Coinbase and plot the market depth.
        """
        configuration_file = 'order_books'
        exchange = 'COINBASE'
        session = get_session(configuration_file)
        Examples.__start_with_except(configuration_file)

        (timestamp,) = session.query(func.max(OrderBookView.time)).first()
        query = session.query(OrderBookView).filter(OrderBookView.exchange == exchange,
                                                    OrderBookView.time == timestamp)
        dataframe = pd.read_sql(query.statement, con=session.bind, index_col='time')
        #
        # bids = dataframe.groupby(pd.cut(dataframe.bids_price, bins=10))['bids_amount'].sum() \
        #     .sort_index(ascending=False).cumsum()
        # asks = dataframe.groupby(pd.cut(dataframe.asks_price, bins=10))['asks_amount'].sum() \
        #     .sort_index(ascending=True).cumsum()
        #
        # index1 = [item.right.__round__() for item in bids.index]
        # index2 = [item.left.__round__() for item in asks.index]
        #
        # # plt.step(index1, bids.values, color='green', label="Bids")
        # # plt.step(index2, asks.values, color="red", label="Asks")
        plt.step(dataframe.bids_price, dataframe.bids_amount.cumsum(), color='green', label='bids')
        plt.step(dataframe.asks_price, dataframe.asks_amount.cumsum(), color='red', label='asks')

        plt.ylim(ymin=0)
        plt.title("Market Depth BTC/USD(T)")
        plt.xlabel("Price in USD(T)")
        plt.ylabel("Accum. Size in BTC")
        plt.legend()
        plt.tight_layout()
        plt.show()


if __name__ == "__main__":
    run()
