import logging
import os
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import List, Tuple, Iterable, Dict, Optional, Union

import tqdm
from pandas import read_sql_query as pd_read_sql_query
from sqlalchemy import create_engine, MetaData, or_, and_, tuple_, func, inspect
from sqlalchemy.exc import ProgrammingError, OperationalError
from sqlalchemy.orm import sessionmaker, Session, Query, aliased
from sqlalchemy_utils import database_exists, create_database

from model.database.tables import ExchangeCurrencyPair, Exchange, Currency, Ticker, HistoricRate, Trade, OrderBook
from model.utilities.exceptions import NotAllPrimaryKeysException
from model.utilities.time_helper import TimeHelper


def _get_exchange_currency_pair(
        session: Session,
        exchange_name: str,
        first_currency_name: str,
        second_currency_name: str) -> Optional[ExchangeCurrencyPair]:
    """
    Checks if there is a currency pair in the database with the given parameters and
    returns it if so.

    @param session: sqlalchemy-session.
    @type session: Session
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


class DatabaseHandler:
    """
    Class which handles every interaction with the database.
    This includes most of the time checking if values exist in
    the database or storing/querying values.

    For querying and storing values the library sqlalchemy is used.

    Attributes:
        sessionFactory: sessionmaker
           Factory for connections to the database.
    """
    sessionFactory: sessionmaker

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
            path: str = None,
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

        logging.info(f"Connection String is: {conn_string}")
        engine = create_engine(conn_string)

        if not database_exists(engine.url):
            create_database(engine.url)
            print(f"Database '{db_name}' created", end="\n\n")
            logging.info(f"Database '{db_name}' created")

        try:  # this is done since one cant test if view-table exists already. if it does an error occurs
            metadata.create_all(engine)
        except (ProgrammingError, OperationalError):
            print("View already exists.")
            logging.warning("Views already exist. If you need to alter or recreate tables delete all views manually.")
            pass
        self.sessionFactory = sessionmaker(bind=engine)

    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        session = self.sessionFactory()
        try:
            yield session
            session.commit()
        except Exception as e:
            # ToDo: Changes here from raise -> pass and included Logging.
            #  Postgresql throw integrity errors which where not caught.
            #  Sqlite on the other hand not. For reproducibility: B2bx, BTC-USD
            logging.exception(e)
            session.rollback()
            pass
            # raise
        finally:
            session.close()

    def get_all_currency_pairs_from_exchange(self, exchange_name: str) -> list[ExchangeCurrencyPair]:
        """
        @param exchange_name: Name of the exchange that the currency-pairs should be queried for.
        @type exchange_name: str

        @return: List of all currency-pairs for the given exchange.
        @rtype: list[ExchangeCurrencyPair]
        """
        with self.session_scope() as session:
            # session.expire_on_commit = False
            currency_pairs = list()
            exchange_id: int = session.query(Exchange.id).filter(Exchange.name == exchange_name.upper()).scalar()
            if exchange_id is not None:
                currency_pairs = session.query(ExchangeCurrencyPair).filter(
                    ExchangeCurrencyPair.exchange_id == exchange_id).all()
                session.expunge_all()
            return currency_pairs

    def get_currency_pairs_with_first_currency(self, exchange_name: str, currency_names: list[str]) \
            -> list[ExchangeCurrencyPair]:
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
        all_found_currency_pairs: list[ExchangeCurrencyPair] = list()
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

    def get_currency_pairs_with_second_currency(self, exchange_name: str, currency_names: list[str]) \
            -> list[ExchangeCurrencyPair]:
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
        all_found_currency_pairs: list[ExchangeCurrencyPair] = list()
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

    def get_currency_pairs(self, exchange_name: str, currency_pairs: list[dict[str, str]]) \
            -> list[ExchangeCurrencyPair]:
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

    def get_exchanges_currency_pairs(self, exchange_name: str, currency_pairs: [Dict[str, str]],
                                     first_currencies: [str], second_currencies: [str]) -> [ExchangeCurrencyPair]:

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
        found_currency_pairs: List[ExchangeCurrencyPair] = list()
        if currency_pairs:
            if "all" in currency_pairs:
                found_currency_pairs.extend(self.get_all_currency_pairs_from_exchange(exchange_name))
            elif currency_pairs[0] is not None:
                found_currency_pairs.extend(self.get_currency_pairs(exchange_name, currency_pairs))

        if first_currencies and second_currencies:
            import itertools
            currency_pairs = list(itertools.product(first_currencies, second_currencies))
            currency_pairs = [{"first": pair[0], "second": pair[1]} for pair in currency_pairs]
            found_currency_pairs.extend(self.get_currency_pairs(exchange_name, currency_pairs))

        elif first_currencies or second_currencies:
            found_currency_pairs.extend(self.get_currency_pairs_with_first_currency(exchange_name, first_currencies))
            found_currency_pairs.extend(self.get_currency_pairs_with_second_currency(exchange_name, second_currencies))

        result: List = list()

        for pair in found_currency_pairs:
            if not any(pair.id == result_pair.id for result_pair in result):
                result.append(pair)
        return result

    def get_exchange_id(self, exchange_name: str) -> int:
        """
        Returns the id of the given exchange if it exists in the database.

        @param exchange_name: str
            Name of the exchange.
        @return:
            Id of the given exchange or None if no exchange with the given name exists
            in the database.
        """

        with self.session_scope() as session:
            return session.query(Exchange.id).filter(Exchange.name == exchange_name.upper()).scalar()

    def get_currency_id(self, currency_name: str):
        """
        Gets the id of a currency.

        @param currency_name:
            Name of the currency.
        @return:
            Id of the given currency or None if no currency with the given name exists
            in the database.
        """

        with self.session_scope() as session:
            return session.query(Currency.id).filter(Currency.name == currency_name.upper()).scalar()

    def persist_exchange(self, exchange_name: str, is_exchange: bool):
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

        # NEVER CALL THIS OUTSIDE OF THIS CLASS

    def persist_exchange_currency_pair(self, exchange_name: str, first_currency_name: str,
                                       second_currency_name: str, is_exchange: bool):
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

    def persist_exchange_currency_pairs(self, currency_pairs: Iterable[Tuple[str, str, str]], is_exchange: bool):
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

            # ex_currency_pairs: List[ExchangeCurrencyPair] = list()
            i = 0
            with self.session_scope() as session:
                for cp in tqdm.tqdm(currency_pairs, disable=(len(currency_pairs) == 1)):
                    exchange_name = cp[0]
                    first_currency_name = cp[1]
                    second_currency_name = cp[2]
                    is_exchange: bool = is_exchange
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
                        # ex_currency_pairs.append(exchange_pair)
                        session.add(exchange_pair)
                    # persist data every 500 CPs in order to avoid slowing down
                    if i % 500 == 0:
                        session.commit()

    def persist_response(self,
                         exchanges_with_pairs: Dict[Exchange, List[ExchangeCurrencyPair]],
                         exchange,
                         db_table,
                         data: Iterable,
                         mappings: List):
        """
        This method persists the given tuples of data. The method currently works for all methods,
        despite currency_pairs. If the program is augmented with a new request-method, use this method to
        persist data.

        The method first gets all columns and primary keys for the request-method from the database.
        As the table-objects are created using **kwargs, it is important that the database column names and the
        .yaml-mapping keys match. Otherwise an exception is raised.

        Further, the method checks if the data tuple contains the 'currency_pair_id'. This is done as some
        exchanges offer to query all data (especially tickers) with one request. The data tuple then does not
        contain the currency_pair_id, as the method Exchange.format_data() does not have any database connection to
        query the respective ids. If the user specified only certain exchange currency pairs in the config-file,
        it would not be filtered but persisted as a whole. Therefore we check for the existence of the
        exchange currency pair replace the currency_pair string with the id or add it, if it does not exist.

        The method then continues to check the existence of each data tuple. The other option, to "ask for forgiveness,
        rather then permission" leads to a lot of "UniqueConstraintError", therefore rollbacks and consequently
        heavily fills up especially PostgreSQL log files. We avoid that by querying the object beforehand.

        If the object is not existing in the database, it will be added.

        @param exchanges_with_pairs: Dict
            Containing the Exchanges and all exchanges_currency_pairs to request specified in the config
        @param exchange: Object
            The Exchange instance from a particular request.
        @param db_table: object
            The database table object for the respective method, i.e. Ticker, OrderBook
        @param data: Iterable, Tuple
            The actual response values
        @param mappings: List
            The mapping keys from the .yaml-file, in the same order as the data-tuples.
        """

        col_names = [key.name for key in inspect(db_table).columns]
        primary_keys = [key.name for key in inspect(db_table).primary_key]
        counter_list = list()
        tuple_counter = 0
        new_pairs: List = list()
        requested_cp_ids = [pair.id for pair in exchanges_with_pairs[exchange]]

        with self.session_scope() as session:
            for data_tuple in data:
                data_tuple = dict(zip(mappings, data_tuple))

                if "exchange_pair_id" not in data_tuple.keys():
                    temp_currency_pair = {"exchange_name": exchange.name,
                                          "first_currency_name": data_tuple["currency_pair_first"],
                                          "second_currency_name": data_tuple["currency_pair_second"]}

                    currency_pair: ExchangeCurrencyPair = _get_exchange_currency_pair(session,
                                                                                      **temp_currency_pair)
                    if not currency_pair:
                        new_pairs.append(temp_currency_pair)

                    if currency_pair and (currency_pair.id in requested_cp_ids):
                        data_tuple.update({"exchange_pair_id": currency_pair.id})
                    else:
                        continue

                data_tuple = {key: data_tuple.get(key) for key in col_names if data_tuple.get(key) is not None}
                check_columns = [pkey in data_tuple.keys() for pkey in primary_keys]
                if not all(check_columns):
                    failed_columns = dict(zip([pkey for pkey in primary_keys], check_columns))
                    logging.exception(NotAllPrimaryKeysException(exchange.name, failed_columns))
                    continue

                p_key_filter = {key: data_tuple.get(key, None) for key in primary_keys}
                query = session.query(db_table).filter_by(**p_key_filter)
                query_exists = session.query(query.exists()).scalar()

                if not query_exists:
                    if db_table.__name__ != Ticker.__name__:
                        counter_list.append(data_tuple["exchange_pair_id"])
                    tuple_counter += 1
                    add_tuple = db_table(**data_tuple)
                    session.add(add_tuple)

        counter_dict = {k: counter_list.count(k) for k in set(counter_list)}
        print(f"{tuple_counter} tuple(s) added to {db_table.__name__} for {exchange.name.capitalize()}.")

        if counter_dict:
            for item in counter_dict.items():
                print(f"CuPair-ID {item[0]}: {item[1]}")

        logging.info(f"{tuple_counter} tuple(s) added to {db_table.__name__} for {exchange.name.capitalize()}.")

        # Persist currency_pairs if not already in the database. This can only happen if an response contains
        # all pairs at once. Problem: Some exchange return "derivatives" which include only a first-currency.
        # Those will be filtered out when updating the currency_pairs in the method Exchange.format_currency_pairs().
        # However, we do not have an instance of
        #
        if len(new_pairs) > 0:
            added_cp_counter = 0
            for item in new_pairs:
                if all(item.values()):
                    self.persist_exchange_currency_pair(**item, is_exchange=exchange.is_exchange)
                    added_cp_counter += 1
            if added_cp_counter > 0:
                print(f"Added {added_cp_counter} new currency pairs to {exchange.name.capitalize()}\n"
                      f"Data will be persisted next time.")
                logging.info(f"Added {added_cp_counter} new currency pairs to {exchange.name.capitalize()}")

        return [item for item in exchanges_with_pairs[exchange] if item.id in counter_dict.keys()]

    def get_readable_query(self,
                           db_table: Union[HistoricRate, OrderBook, Ticker, Trade],
                           query_everything: bool,
                           from_timestamp: datetime = None,
                           to_timestamp: datetime = TimeHelper.now(),
                           exchanges: List[str] = None,
                           currency_pairs: List[Dict[str, str]] = None,
                           first_currencies: List[str] = None,
                           second_currencies: List[str] = None):

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
             @param db_table: object
                 The respective object of the table to be queried (i.e. Ticker, Trade,...).
             @param query_everything: bool
                 If everything in the database should be queried.
             @param from_timestamp: datetime
                 Minimum date for the start of the request.
             @param to_timestamp: datetime
                 Maximum date for the start of the request.
             @param exchanges: List[str]
                 List of exchanges of which the tuple should be queried.
             @param currency_pairs: List[Dict[str, str]]
                 List of specific currency pairs that should be queried.
                 Dict needs to have the following structure:
                     - first: 'Name of the first currency'
                       second: 'Name of the second currency'
             @param first_currencies: List[str]
                 List of viable currencies for the first currency in a currency pair.
             @param second_currencies: List[str]
                 List of viable currencies for the second currency in a currency pair.
             @return:
                 List of readable database tuple.
                 List might be empty if database is empty or there where no ExchangeCurrencyPairs
                 which fulfill the above stated requirements.
             """

        with self.session_scope() as session:
            first = aliased(Currency)
            second = aliased(Currency)
            # col_names = [key.name for key in inspect(db_table).columns]

            data: Query = session.query(Exchange.name.label('exchange'),
                                        first.name.label('first_currency'),
                                        second.name.label('second_currency'),
                                        db_table). \
                join(ExchangeCurrencyPair, db_table.exchange_pair_id == ExchangeCurrencyPair.id). \
                join(Exchange, ExchangeCurrencyPair.exchange_id == Exchange.id). \
                join(first, ExchangeCurrencyPair.first_id == first.id). \
                join(second, ExchangeCurrencyPair.second_id == second.id)  # .options(joinedload('exchange_pair'))

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
                        currency_pairs_names = [(pair['first'].upper(), pair['second'].upper()) for pair in
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

                result = pd_read_sql_query(data.statement, con=session.bind)
            session.expunge_all()
        return result

    def get_first_timestamp(self, table: Union[HistoricRate, OrderBook, Ticker, Trade], exchange_pair_id: int):
        """
        Returns the earliest timestamp from the specified table if the latest timestamp is less than 2 days old or
        otherwise the timestamp from now.

        @param table: The database table to be queried.
        @param exchange_pair_id: The exchange_pair_id of interest.

        @return: datetime: Earliest timestamp of specified table or timestamp from now.
        """
        with self.session_scope() as session:
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
        else:
            return TimeHelper.now()
