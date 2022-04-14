#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Module for interacting with the database.

Classes:
    - DatabaseHandler:
        Methods:
            - Constructor
            - Session-Factory
            - Get methods
            - Persist methods
"""

import importlib
import logging
import os
from contextlib import contextmanager
from datetime import datetime, timedelta
from itertools import product
from typing import List, Iterable, Optional, Generator, Any, Iterator, Dict, Tuple, Union

import sqlalchemy.orm
from pandas import DataFrame
from pandas import read_sql_query as pd_read_sql_query
from sqlalchemy import create_engine, MetaData, or_, and_, tuple_, func, inspect
from sqlalchemy.exc import ProgrammingError, OperationalError, SQLAlchemyError
from sqlalchemy.orm import sessionmaker, Session, Query, aliased
from sqlalchemy_utils import database_exists, create_database

from model.database.tables import ExchangeCurrencyPair, Exchange, Currency, DatabaseTable
from model.utilities.time_helper import TimeHelper, TimeUnit
from model.utilities.utilities import split_str_to_list


class DatabaseHandler:
    """
    Class which handles every interaction with the database.
    This includes most of the time checking if values exist in
    the database or storing/querying values.

    For querying and storing values the library sqlalchemy is used.

    Attributes:
        session_factory: sessionmaker
           Factory for connections to the database.
    """

    def __init__(
            self,
            metadata: MetaData,
            sqltype: str,
            client: str,
            user_name: str,
            password: str,
            host: str,
            port: str,
            db_name: str,
            min_return_tuples: int = 1,
            path: Optional[str] = None,
            debug: bool = False):
        """
        Initializes the database-handler.

        Builds the connection-string and tries to connect to the database.

        Creates with the given metadata tables which do not already exist.
        Won't make a new table if the name already exists,
        so changes to the table-structure have to be made by hand in the database
        or the table has to be deleted.

        Initializes the sessionFactory with the created engine.
        Engine variable is no attribute and currently only exists in the constructor.

        @param metadata: Metadata Information about the table-structure of the database.
                        See tables.py for more information.
        @type metadata: MetaData
        @param sqltype: Type of the database sql-dialect. ('postgresql, mariadb, mysql, sqlite' for us)
        @type sqltype: str
        @param client: Name of the Client which is used to connect to the database.
        @type client: str
        @param user_name: Username under which this program connects to the database.
        @type user_name: str
        @param password: Password for this username.
        @type password: str
        @param host: Hostname or host-address from the database.
        @type host: str
        @param port: Connection-Port (usually 5432 for Postgres)
        @type port: str
        @param db_name: Name of the database.
        @type db_name: str
        @param min_return_tuples: Minimum amount of tuples returned in order to keep exchange alive
        @type min_return_tuples: int
        @param path: Path to the database directory.
        @type path: str
        @param debug: Indicates if the debug mode is on.
        @type debug: bool
        """
        if not path:
            path = os.getcwd()

        conn_strings = {"debug": "sqlite://",
                        "sqlite": f"{sqltype}:///{path}/{db_name}.db",
                        "postgresql": f"{sqltype}+{client}://{user_name}:{password}@{host}:{port}/{db_name}",
                        "mariadb": f"{sqltype}+{client}://{user_name}:{password}@{host}:{port}/{db_name}",
                        "mysql": f"{sqltype}+{client}://{user_name}:{password}@{host}:{port}/{db_name}"}
        if debug:
            conn_string = conn_strings["debug"]
        else:
            conn_string = conn_strings[sqltype]

        logging.info("Connection String is: %s", conn_string)
        engine = create_engine(conn_string)

        if not database_exists(engine.url):
            create_database(engine.url)
            print(f"Database {db_name} created.", end="\n\n")
            logging.info("Database '%s' created", db_name)

        try:  # this is done since one can't test if view-table exists already. if it does an error occurs
            metadata.create_all(engine)
        except (ProgrammingError, OperationalError):
            message = "Database Views already exist. If you need to alter or recreate tables delete all views manually."
            logging.warning(message)

        self.session_factory: sessionmaker = sessionmaker(bind=engine)
        self._min_return_tuples = min_return_tuples

        if sqltype == 'mariadb':
            sqltype = "mysql"
        self.insert_module = importlib.import_module(f"sqlalchemy.dialects.{sqltype}")


    @contextmanager
    def session_scope(self) -> Generator[Session, None, None]:
        """Provide a transactional scope around a series of operations."""
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except SQLAlchemyError as ex:
            #  Postgresql throw integrity errors which where not caught.
            #  Sqlite on the other hand not. For reproducibility: B2bx, BTC-USD
            logging.exception(ex)
            session.rollback()
        finally:
            session.close()

    def get_currency_id(self, currency_name: str) -> Optional[int]:
        """
        Gets the ID of the given currency if it exists in the database.

        @param currency_name: The name of the currency.

        @return: The ID of the given currency or None if no currency with the given name exists in the database.
        """
        with self.session_scope() as session:
            return session.query(Currency.id).filter(Currency.name == currency_name.upper()).scalar()

    def get_exchange_id(self, exchange_name: str) -> int:
        """
        Gets the ID of the given exchange if it exists in the database.

        @param exchange_name: The name of the exchange.

        @return: The ID of the given exchange or None if no exchange with the given name exists in the database.
        """
        with self.session_scope() as session:
            return session.query(Exchange.id).filter(Exchange.name == exchange_name.upper()).scalar()

    def get_currency_pairs(self, exchange_name: str, currency_pairs: List[Dict[str, str]]) \
            -> List[ExchangeCurrencyPair]:
        """
        Returns all ExchangeCurrencyPairs for the given exchange if they fit any
        currency pairs in the given list of dictionaries.

        @param exchange_name: Name of the exchange.
        @type exchange_name: str
        @param currency_pairs: List of the currency pairs that should be found.
                               Each dictionary should contain the keys 'first' and 'second'
                               which contain the names of the currencies.
        @type currency_pairs: list[dict[str, str]]

        @return: List of all found currency pairs on this exchange based on the given pair combinations.
        @rtype: list[ExchangeCurrencyPair]
        """
        found_currency_pairs: List[ExchangeCurrencyPair] = list()

        if exchange_name:
            exchange_id: int = self.get_exchange_id(exchange_name)
            with self.session_scope() as session:
                if currency_pairs is not None:
                    for currency_pair in currency_pairs:
                        first_currency = currency_pair["first"]
                        second_currency = currency_pair["second"]
                        if first_currency and second_currency:
                            first_id: int = self.get_currency_id(first_currency)
                            second_id: int = self.get_currency_id(second_currency)

                            found_currency_pair = session.query(ExchangeCurrencyPair).filter(
                                ExchangeCurrencyPair.exchange_id == exchange_id,
                                ExchangeCurrencyPair.first_id == first_id,
                                ExchangeCurrencyPair.second_id == second_id).first()

                            if found_currency_pair is not None:
                                found_currency_pairs.append(found_currency_pair)
                    session.expunge_all()

        return found_currency_pairs

    @staticmethod
    def _get_exchange_currency_pair(session: sqlalchemy.orm.Session,
                                    exchange_name: str,
                                    first_currency_name: str,
                                    second_currency_name: str) -> Optional[ExchangeCurrencyPair]:
        """
        Checks if there is a currency pair in the database with the given parameters and
        returns it if so.

        @param session: Session from the session-factory
        @type: session: sqlalchemy.orm.Session
        @param exchange_name: Name of the exchange.
        @type exchange_name: str
        @param first_currency_name: Name of the first currency in the currency pair.
        @type first_currency_name: str
        @param second_currency_name: Name of the second currency in the currency pair.
        @type second_currency_name: str
        @return: The ExchangeCurrencyPair which fulfills all the requirements or None
                 if no such ExchangeCurrencyPair exists.
        @rtype: Optional[ExchangeCurrencyPair]
        """
        if exchange_name is None or first_currency_name is None or second_currency_name is None:
            return None

        ex = session.query(Exchange).filter(Exchange.name == exchange_name.upper()).first()
        first = session.query(Currency).filter(Currency.name == first_currency_name.upper()).first()
        second = session.query(Currency).filter(Currency.name == second_currency_name.upper()).first()

        return session.query(ExchangeCurrencyPair).filter(
            ExchangeCurrencyPair.exchange == ex,
            ExchangeCurrencyPair.first == first,
            ExchangeCurrencyPair.second == second
        ).first()

    def get_exchanges_currency_pairs(self,
                                     exchange_name: str,
                                     currency_pairs: List[Dict[str, str]],
                                     first_currencies: Union[List[str], str],
                                     second_currencies: Union[List[str], str]) -> List[ExchangeCurrencyPair]:

        """
        Collects and returns all currency pairs for the given exchange that either have any
        of the currencies of first_currencies/second_currencies as a currency as
        first/second or match a specific pair in currency_pairs.

        @param exchange_name: str
            Name of the exchange.
        @param currency_pairs: List[Dict[str, str]]
            List of specific currency pairs that should be found.
            Dictionary should have the following keys:
                first: 'name of the first currency'
                second: 'name of the second currency'
        @param first_currencies: List[str]
            List of currency names that are viable as first currency.
            All pairs that have any of the given names as first currency will be returned.
        @param second_currencies: List[str]
            List of currency names that are viable as second currency.
            All pairs that have any of the given names as second currency will be returned.
        @return:
            All ExchangeCurrencyPairs of the given Exchange that fulfill any
            of the above stated conditions.
        """
        if isinstance(first_currencies, str):
            first_currencies = split_str_to_list(first_currencies)

        if isinstance(second_currencies, str):
            second_currencies = split_str_to_list(second_currencies)

        found_currency_pairs: List[ExchangeCurrencyPair] = list()

        if currency_pairs:

            if "all" in currency_pairs:
                found_currency_pairs.extend(self.get_all_currency_pairs_from_exchange(exchange_name))
            elif isinstance(currency_pairs, str):
                currency_pairs = split_str_to_list(currency_pairs)
                currency_pairs = [{"first": split_str_to_list(pair, "-")[0],
                                   "second": split_str_to_list(pair, "-")[-1]} for pair in currency_pairs]
                found_currency_pairs.extend(self.get_currency_pairs(exchange_name, currency_pairs))

        if first_currencies and second_currencies:
            currency_pairs = list(product(first_currencies, second_currencies))
            currency_pairs = [{"first": pair[0], "second": pair[1]} for pair in currency_pairs]
            found_currency_pairs.extend(self.get_currency_pairs(exchange_name, currency_pairs))

        elif first_currencies or second_currencies:
            found_currency_pairs.extend(self.get_currency_pairs_with_first_currency(exchange_name, first_currencies))
            found_currency_pairs.extend(self.get_currency_pairs_with_second_currency(exchange_name, second_currencies))

        result: List[ExchangeCurrencyPair] = list()

        for pair in found_currency_pairs:
            if not any(pair.id == result_pair.id for result_pair in result):
                result.append(pair)
        return result

    def get_all_currency_pairs_from_exchange(self, exchange_name: str) -> List[ExchangeCurrencyPair]:
        """
        @param exchange_name: Name of the exchange that the currency-pairs should be queried for.
        @type exchange_name: str

        @return: List of all currency-pairs for the given exchange.
        @rtype: list[ExchangeCurrencyPair]
        """
        with self.session_scope() as session:
            currency_pairs = list()
            exchange_id: int = session.query(Exchange.id).filter(Exchange.name == exchange_name.upper()).scalar()
            if exchange_id is not None:
                currency_pairs = session.query(ExchangeCurrencyPair).filter(
                    ExchangeCurrencyPair.exchange_id == exchange_id).all()
                session.expunge_all()

        return currency_pairs

    def get_currency_pairs_with_first_currency(self, exchange_name: str, currency_names: List[str]) \
            -> List[ExchangeCurrencyPair]:
        """
        Returns all currency-pairs for the given exchange that have any of the given currencies
        as the first currency.

        @param exchange_name: Name of the exchange.
        @type exchange_name: str
        @param currency_names: List of the currency names that are viable as first-currencies.
        @type currency_names: list[str]

        @return: List of the currency-pairs which start with any of the currencies in currency_names
                 on the given exchange.
                 List is empty if there are no currency pairs in the database which fulfill the requirements.
        @rtype: list[ExchangeCurrencyPair]
        """
        if isinstance(currency_names, str):
            currency_names = [currency_names]
        all_found_currency_pairs: List[ExchangeCurrencyPair] = list()
        if exchange_name is not None and exchange_name:
            exchange_id: int = self.get_exchange_id(exchange_name)

            with self.session_scope() as session:
                if currency_names is not None:
                    for currency_name in currency_names:
                        if currency_name is not None and currency_name:
                            first_id: int = self.get_currency_id(currency_name)

                            found_currency_pairs = session.query(ExchangeCurrencyPair).filter(
                                ExchangeCurrencyPair.exchange_id == exchange_id,
                                ExchangeCurrencyPair.first_id == first_id).all()

                            if found_currency_pairs is not None:
                                all_found_currency_pairs.extend(found_currency_pairs)
                session.expunge_all()
        return all_found_currency_pairs

    def get_currency_pairs_with_second_currency(self, exchange_name: str, currency_names: List[str]) \
            -> List[ExchangeCurrencyPair]:
        """
        Returns all currency-pairs for the given exchange that have any of the given currencies
        as the second currency.

        @param exchange_name: Name of the exchange.
        @type exchange_name: str
        @param currency_names: List of the currency names that are viable as second currencies.
        @type currency_names: list[str]

        @return: List of the currency-pairs which end with any of the currencies in currency_names
                 on the given exchange.
                 List is empty if there are no currency pairs in the database which fulfill the requirements.
        @rtype: list[ExchangeCurrencyPair]
        """
        all_found_currency_pairs: List[ExchangeCurrencyPair] = list()
        if exchange_name:
            exchange_id: int = self.get_exchange_id(exchange_name)

            with self.session_scope() as session:
                if currency_names is not None:
                    for currency_name in currency_names:
                        if currency_name:
                            second_id: int = self.get_currency_id(currency_name)

                            found_currency_pairs = session.query(ExchangeCurrencyPair).filter(
                                ExchangeCurrencyPair.exchange_id == exchange_id,
                                ExchangeCurrencyPair.second_id == second_id).all()

                            if found_currency_pairs is not None:
                                all_found_currency_pairs.extend(found_currency_pairs)

                session.expunge_all()
        return all_found_currency_pairs

    def get_readable_query(self,
                           db_table: DatabaseTable,
                           query_everything: bool,
                           from_timestamp: datetime = None,
                           to_timestamp: datetime = TimeHelper.now(),
                           exchanges: List[str] = None,
                           currency_pairs: List[Dict[str, str]] = None,
                           first_currencies: List[str] = None,
                           second_currencies: List[str] = None) -> DataFrame:

        """
             Queries based on the parameters readable database data and returns it.
             If query_everything is true, everything ticker tuple will be returned.
             This is also the case if query_everything is false but there were no
             exchanges or currencies/currency pairs given.
             If exchanges are given only tuples of these given exchanges will be returned.
             If there are no currencies/currency pairs given,
             all ticker-tuple of the given exchange will be returned.
             If currencies are given note that only ticker tuple with currency pairs,
             which have either any currency in first_currencies as first OR any currency
             in second_currencies as second OR any currency pairs in currency_pairs will be returned.
             If timestamps are given the queried tuples will be filtered accordingly.

             So query logic for each tuple is (if exchange, currencies and time are given):
                 exchange AND (first OR second OR pair) AND from_time AND to_time

             See csv-config for details of how to write/give parameters.

             @param db_table: The respective object of the table to be queried.
             @type db_table: Union[HistoricRate, OrderBook, Ticker, Trade]
             @param query_everything: If everything in the database should be queried.
             @type query_everything: bool
             @param from_timestamp: Minimum date for the start of the request.
             @type from_timestamp: datetime
             @param to_timestamp: Maximum date for the start of the request.
             @type to_timestamp: datetime
             @param exchanges: List of exchanges of which the tuple should be queried.
             @type exchanges: list[str]
             @param currency_pairs: List of specific currency pairs that should be queried.
                                    Dict needs to have the following structure:
                                       - first: 'Name of the first currency'
                                       - second: 'Name of the second currency'
             @type currency_pairs: list[dict[str, str]]
             @param first_currencies: List of viable currencies for the first currency in a currency pair.
             @type first_currencies: list[str]
             @param second_currencies: List of viable currencies for the second currency in a currency pair.
             @type second_currencies: list[str]

             @return: DataFrame of readable database tuple.
                      DataFrame might be empty if database is empty or there where no ExchangeCurrencyPairs
                      which fulfill the above stated requirements.
             @rtype: Pandas DataFrame
             """
        with self.session_scope() as session:
            first = aliased(Currency)
            second = aliased(Currency)

            data: Query = session.query(Exchange.name.label("exchange"),
                                        first.name.label("first_currency"),
                                        second.name.label("second_currency"),
                                        db_table). \
                join(ExchangeCurrencyPair, db_table.exchange_pair_id == ExchangeCurrencyPair.id). \
                join(Exchange, ExchangeCurrencyPair.exchange_id == Exchange.id). \
                join(first, ExchangeCurrencyPair.first_id == first.id). \
                join(second, ExchangeCurrencyPair.second_id == second.id)

            if query_everything:
                result = pd_read_sql_query(data.statement, con=session.bind)
            else:
                if exchanges:
                    exchange_names = [name.upper() for name in exchanges]
                else:
                    exchange_names = [r[0] for r in session.query(Exchange.name)]
                if not first_currencies and not second_currencies and not currency_pairs:
                    first_currency_names = [r[0] for r in session.query(Currency.name)]
                else:
                    if first_currencies:
                        first_currency_names = [name.upper() for name in first_currencies]
                    if second_currencies:
                        second_currency_names = [name.upper() for name in second_currencies]
                    if currency_pairs:
                        currency_pairs_names = [(pair["first"].upper(), pair["second"].upper()) for pair in
                                                currency_pairs]

                result = data.filter(and_(
                    Exchange.name.in_(exchange_names),
                    or_(
                        first.name.in_(first_currency_names),  # first currency
                        second.name.in_(second_currency_names),  # second currency
                        tuple_(first.name, second.name).in_(currency_pairs_names)  # currency_pair
                    ),
                ))

                if from_timestamp:
                    result = result.filter(db_table.time >= from_timestamp)
                if to_timestamp:
                    result = result.filter(db_table.time <= to_timestamp)

                # TODO: Philipp: Ask Steffen because result is not used?

                result = pd_read_sql_query(data.statement, con=session.bind)
            session.expunge_all()
        return result

    def get_or_create_exchange_pair_id(self,
                                       exchange_name: str,
                                       first_currency_name: str,
                                       second_currency_name: str,
                                       is_exchange: bool) -> int:
        """
        Returns an existing exchange-currency-pair id or creates a new instance and returns the id.

        @param exchange_name: Exchange name
        @param first_currency_name: First currency name
        @param second_currency_name: Second currency name
        @param is_exchange: Is from exchange or platform

        @return: Id of the existing or newly created exchange currency pair
        """
        temp_currency_pair = {"exchange_name": exchange_name,
                              "first_currency_name": first_currency_name,
                              "second_currency_name": second_currency_name}

        with self.session_scope() as session:
            currency_pair: ExchangeCurrencyPair = self._get_exchange_currency_pair(session, **temp_currency_pair)

            if not currency_pair and all([exchange_name, first_currency_name, second_currency_name, is_exchange]):
                self._persist_exchange_currency_pair(is_exchange=is_exchange, **temp_currency_pair)
                return self.get_or_create_exchange_pair_id(is_exchange=is_exchange, **temp_currency_pair)
            else:
                return currency_pair.id

    def get_first_timestamp(self, table: DatabaseTable, exchange_pair_id: int, last_row_id: int) -> datetime:
        """
        Returns the earliest timestamp from the specified table if the latest timestamp is less than 2 days old.
        If the table is empty, the method trys to catch information from the helper table PairInfo.
        Otherwise, the timestamp from now.

        @param table: The database table to be queried.
        @type table: Union[HistoricRate, OrderBook, Ticker, Trade]
        @param exchange_pair_id: The exchange_pair_id of interest.
        @type exchange_pair_id: int
        @param last_row_id: The row-id of the last entry of previous request.
        @type: last_row_id: int
        @return: datetime: Earliest timestamp of specified table or timestamp from now.
        @rtype: datetime
        """
        with self.session_scope() as session:
            if last_row_id:
                timestamp = session.execute(f"SELECT time FROM historic_rates where rowid = {last_row_id} "
                                            f"ORDER BY time DESC").first()[0]
                return TimeHelper.from_timestamp(timestamp, TimeUnit.MILLISECONDS)

            (earliest_timestamp,) = session \
                .query(func.min(table.time)) \
                .filter(table.exchange_pair_id == exchange_pair_id) \
                .first()
            (oldest_timestamp,) = session \
                .query(func.max(table.time)) \
                .filter(table.exchange_pair_id == exchange_pair_id) \
                .first()

        # two days as some exchanges lag behind one day for historic_rates
        if earliest_timestamp and (TimeHelper.now() - oldest_timestamp) < timedelta(days=2):
            return earliest_timestamp

        return TimeHelper.now()

    def persist_exchange(self, exchange_name: str, is_exchange: bool) -> None:
        """
        Persists the given exchange-name if it's not already in the database.

        @param exchange_name:
            Name that should is to persist.
        @param is_exchange: boolean indicating if the exchange is indeed an exchange or a platform
        """
        with self.session_scope() as session:
            exchange_id = session.query(Exchange.id).filter(Exchange.name == exchange_name.upper()).first()
            if exchange_id is None:
                exchange = Exchange(name=exchange_name, is_exchange=is_exchange)
                session.add(exchange)

    def _persist_exchange_currency_pair(self,
                                        exchange_name: str,
                                        first_currency_name: str,
                                        second_currency_name: str,
                                        is_exchange: bool) -> None:
        """
        Adds a single ExchangeCurrencyPair to the database is it does not already exist.

        @param exchange_name: str
            Name of the exchange.
        @param first_currency_name: str
            Name of the first currency.
        @param second_currency_name: str
            Name of the second currency.
        @param is_exchange: boolean indicating if the exchange is indeed an exchange or a platform
        """
        self.persist_exchange_currency_pairs([(exchange_name, first_currency_name, second_currency_name)],
                                             is_exchange=is_exchange)

    def persist_exchange_currency_pairs(self, currency_pairs: Iterable[Tuple[str, str, str]],
                                        is_exchange: bool) -> None:
        """
        Persists the given already formatted ExchangeCurrencyPair-tuple if they not already exist.
        The formatting ist done in @see{Exchange.format_currency_pairs()}.

        Tuple needs to have the following structure:
            (exchange-name, first currency-name, second currency-name)

        @param currency_pairs:
            Iterator of currency-pair tuple that are to persist.
        @param is_exchange: boolean indicating if the exchange is indeed an exchange or a platform
        """
        if currency_pairs is not None:

            i = 0
            with self.session_scope() as session:
                for currency_pair in currency_pairs:
                    exchange_name = currency_pair[0]
                    first_currency_name = currency_pair[1]
                    second_currency_name = currency_pair[2]
                    i += 1

                    if any([exchange_name, first_currency_name, second_currency_name]) is None:
                        continue

                    if first_currency_name == second_currency_name:
                        continue

                    existing_exchange = session.query(Exchange).filter(Exchange.name == exchange_name.upper()).first()
                    exchange: Exchange = existing_exchange if existing_exchange is not None else Exchange(
                        name=exchange_name, is_exchange=is_exchange)

                    existing_first_cp = session.query(Currency).filter(
                        Currency.name == first_currency_name.upper()).first()
                    first: Currency = existing_first_cp if existing_first_cp is not None else Currency(
                        name=first_currency_name, from_exchange=is_exchange)

                    existing_second_cp = session.query(Currency).filter(
                        Currency.name == second_currency_name.upper()).first()
                    second: Currency = existing_second_cp if existing_second_cp is not None else Currency(
                        name=second_currency_name, from_exchange=is_exchange)

                    existing_exchange_pair = session.query(ExchangeCurrencyPair).filter(
                        ExchangeCurrencyPair.exchange_id == exchange.id,
                        ExchangeCurrencyPair.first_id == first.id,
                        ExchangeCurrencyPair.second_id == second.id)

                    existing_exchange_pair = session.query(existing_exchange_pair.exists()).scalar()

                    if not existing_exchange_pair:
                        exchange_pair = ExchangeCurrencyPair(exchange=exchange, first=first, second=second)
                        session.add(exchange_pair)
                    # persist data every 500 CPs in order to avoid slowing down
                    if i % 500 == 0:
                        session.commit()

    def persist_response(self,
                         exchanges_with_pairs: Dict[Exchange, Dict[ExchangeCurrencyPair, Optional[int]]],
                         exchange: Exchange,
                         db_table: DatabaseTable,
                         formatted_response: Iterator[Any]) -> Dict[ExchangeCurrencyPair, Optional[int]]:
        """
        Method to persist the formatted response into the database. Every data tuple get is inspected for
        valid data, i.e. the mapping key and corresponding data must be one of the table columns of the database.
        Furthermore, if the data tuple does not contain an exchange_pair_id, a new ExchangeCurrencyPair is
        persisted into the database. To persist data, this method makes use of:
            - sqlalchemy.dialects.<dialect>.insert
        which specifically allows for:
            - on_conflict_do_nothing, or
            - on_conflict_do_update.
        By default, conflicts are ignored, however, if existing rows are supposed to be updated,
        the behaviour can be changed.

        @param exchanges_with_pairs: Dict containing all requested exchanges and pairs
        @param exchange: Exchange Object
        @param db_table: Affected database table, i.e. request-method
        @param formatted_response: Generator of extracted and formatted response.
        @return: Dict containing ExchangeCurrencyPair-Object and the last inserted row_id
        """
        col_names = [key.name for key in inspect(db_table).columns]
        primary_keys = [key.name for key in inspect(db_table).primary_key]
        counter_dict: Dict[int, int] = dict()
        requested_cp_ids = [pair.id for pair in exchanges_with_pairs[exchange]]

        while True:
            try:
                data_to_persist: List[dict, ...] = list()
                data, mappings = next(formatted_response)

                for data_tuple in data:
                    data_tuple = dict(zip(mappings, data_tuple))

                    if "exchange_pair_id" not in data_tuple.keys():
                        temp_pair = {"exchange_name": exchange.name,
                                     "first_currency_name": data_tuple["currency_pair_first"],
                                     "second_currency_name": data_tuple["currency_pair_second"],
                                     "is_exchange": exchange.is_exchange}

                        new_pair_id = self.get_or_create_exchange_pair_id(**temp_pair)
                        if new_pair_id in requested_cp_ids:
                            data_tuple.update({"exchange_pair_id": new_pair_id})
                        else:
                            continue

                    data_tuple = {key: data_tuple.get(key, None) for key in col_names}
                    data_to_persist.append(data_tuple)
                    exchange_pair_id = [item.get("exchange_pair_id") for item in data_to_persist]
                    # remove duplicates
                    exchange_pair_id = list(dict.fromkeys(exchange_pair_id))

                if not data_to_persist:
                    continue

                # Sort data by timestamp in order to ensure the last_row_id (see below) to be with the oldest timestamp.
                # This is used for historic_rates.get_first_timestamp(), if the oldest timestamp of the previous
                # request is wanted, instead of the oldest timestamp in the database.
                data_to_persist = sorted(data_to_persist, key=lambda i: i["time"], reverse=True)

                with self.session_scope() as session:

                    stmt = self.insert_module.insert(db_table).values(data_to_persist)

                    try:
                        stmt = stmt.on_conflict_do_nothing(index_elements=primary_keys)
                    except AttributeError:
                        stmt = stmt.prefix_with("IGNORE")

                    row_count = session.execute(stmt)

                    print(f"Pair-ID {exchange_pair_id[0] if len(exchange_pair_id) == 1 else 'ALL'}"
                          f" - {exchange.name.capitalize()}: {row_count.rowcount} tuple(s)")

                    # Dict containing the ExchangeCurrencyPair as key and the last_row_id as value, if and only if
                    # at least self._min_return_tuples are persisted. If not, the ExchangeCurrencyPair will be kicked
                    # out in the next run. The strange subscription of the dict-comprehension is because of the nested
                    # dict in exchange_with_pairs: Dict[Exchange, Dict[ExchangeCurrencyPair, Optional[int]]].
                    counter_dict.update({k: row_count.lastrowid for k, v in exchanges_with_pairs[exchange].items()
                                         if k.id in exchange_pair_id and row_count.rowcount >= self._min_return_tuples})

            except StopIteration:
                break

        return counter_dict if counter_dict else {}
