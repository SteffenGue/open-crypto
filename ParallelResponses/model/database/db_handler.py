import logging
from datetime import datetime
from typing import List, Tuple, Iterator, Iterable, Dict
from contextlib import contextmanager
import tqdm
import psycopg2
import sqlalchemy
from pandas import read_sql_query as pd_read_sql_query
from sqlalchemy import create_engine, MetaData, or_, and_, tuple_, inspect
from sqlalchemy.exc import ProgrammingError, OperationalError
from sqlalchemy.orm import sessionmaker, Session, Query, aliased, joinedload
from sqlalchemy_utils import database_exists, create_database

from model.database.tables import Currency, Exchange, ExchangeCurrencyPair, Ticker


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

    def __init__(self,
                 metadata: MetaData,
                 sqltype: str,
                 client: str,
                 user_name: str,
                 password: str,
                 host: str,
                 port: str,
                 db_name: str):
        """
        Initializes the database-handler.

        Builds the connection-string and tries to connect to the database.

        Creates with the given metadata tables which do not already exist.
        Won't make a new table if the name already exists,
        so changes to the table-structure have to be made by hand in the database
        or the table has to be deleted.

        Initializes the sessionFactory with the created engine.
        Engine variable is no attribute and currently only exists in the constructor.

        @param metadata: Metadata
            Information about the table-structure of the database.
            See tables.py for more information.
        @param sqltype: atr
            Type of the database sql-dialect. ('postgresql, sqlite' for us)
        @param client: str
            Name of the Client which is used to connect to the database.
        @param user_name: str
            Username under which this program connects to the database.
        @param password: str
            Password for this username.
        @param host: str
            Hostname or Hostaddress from the database.
        @param port: str
            Connection-Port (usually 5432 for Postgres)
        @param db_name: str
            Name of the database.
        """
        if sqltype == 'sqlite':
            conn_string = '{}:///{}.db'.format(sqltype, db_name)
        else:
            conn_string = '{}+{}://{}:{}@{}:{}/{}'.format(sqltype, client, user_name, password, host, port, db_name)
        logging.info('Connection String is: {}'.format(conn_string))
        engine = create_engine(conn_string)

        if not database_exists(engine.url):
            create_database(engine.url)
            print(f"Database '{db_name}' created", end="\n\n")
            logging.info(f"Database '{db_name}' created")

        try:  # this is done since one cant test if view-table exists already. if it does an error occurs
            metadata.create_all(engine)
        except (ProgrammingError, OperationalError):
            print('View already exists.')
            logging.warning('Views already exist. If you need to alter or recreate tables delete tickers_view '
                            'and exchanges_currency_pair_view manually.')
            pass
        self.sessionFactory = sessionmaker(bind=engine)

    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        session = self.sessionFactory()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()


    def persist_tickers(self,
                        queried_currency_pairs: List[ExchangeCurrencyPair],
                        tickers: Iterator[Tuple[str, datetime, datetime, str, str, float, float, float, float]]):
        """
        Persists the given tuples of ticker-data.

        The method checks for each tuple if the referenced exchange and
        currencies exist in the database.
        If so, the Method creates with the stored data of the current tuple
        a new Ticker-object which is then added to the commit.
        After all tuples where checked, the added Ticker-objects will be
        committed and the connection will be closed.

        Exceptions will be caught but not really handled.
        TODO: Exception handling and
        TODO: Logging of Exception

        @param tickers: Iterator
            Iterator of tuples containing ticker-data.
            Tuple must have the following structure:
                (exchange-name,
                 start_time,
                 time,
                 first_currency_symbol,
                 second_currency_symbol,
                 ticker_last_price,
                 ticker_best_ask,
                 ticker_best_bid,
                 ticker_daily_volume)
        @param queried_currency_pairs: List of all queried ExchangeCurrencyPairs from the database
        """

        with self.session_scope() as session:
            tuple_counter: int = 0
            for ticker in tickers:
                exchange_currency_pair: ExchangeCurrencyPair = self.get_exchange_currency_pair(session, ticker[0],
                                                                                               ticker[3], ticker[4])
                if exchange_currency_pair is not None:
                    if any(exchange_currency_pair.id == q_cp.id for q_cp in queried_currency_pairs):
                        # todo: was ist wenn man alle cps holen will, oder wenn keins angegeben ist??
                        ticker_tuple = Ticker(exchange_pair_id=exchange_currency_pair.id,
                                              exchange_pair=exchange_currency_pair,
                                              start_time=ticker[1],
                                              time=ticker[2],
                                              last_price=ticker[5],
                                              best_ask=ticker[6],
                                              best_bid=ticker[7],
                                              daily_volume=ticker[8])
                        tuple_counter += 1
                        session.add(ticker_tuple)
            print('{} ticker added for {}.'.format(tuple_counter, ticker[0].capitalize()))
            logging.info('{} ticker added for {}.'.format(tuple_counter, ticker[0].capitalize()))
            return tuple_counter

    def get_all_currency_pairs_from_exchange(self, exchange_name: str) -> List[ExchangeCurrencyPair]:
        """
        @param exchange_name:
            Name of the exchange that the currency-pairs should be queried for.
        @return:
            List of all currency-pairs for the given exchange.
        """
        # todo : session factory -> session scope
        with self.session_scope() as session:
            # session.expire_on_commit = False
            currency_pairs = list()
            exchange_id: int = session.query(Exchange.id).filter(Exchange.name.__eq__(exchange_name.upper())).scalar()
            if exchange_id is not None:
                currency_pairs = session.query(ExchangeCurrencyPair).filter(
                    ExchangeCurrencyPair.exchange_id.__eq__(exchange_id)).all()
                session.expunge_all()
            return currency_pairs

    def get_currency_pairs_with_first_currency(self, exchange_name: str, currency_names: [str]) \
            -> List[ExchangeCurrencyPair]:
        """
        Returns all currency-pairs for the given exchange that have any of the given currencies
        as the first currency.

        @param exchange_name: str
            Name of the exchange.
        @param currency_names: List[str]
            List of the currency names that are viable as first-currencies.
        @return:
            List of the currency-pairs which start with any of the currencies in currency_names
            on the given exchange.
            List is empty if there are no currency pairs in the database which fulfill the requirements.
        """
        all_found_currency_pairs: List[ExchangeCurrencyPair] = list()
        if exchange_name is not None and exchange_name:
            exchange_id: int = self.get_exchange_id(exchange_name)

            with self.session_scope() as session:
                if currency_names is not None:
                    for currency_name in currency_names:
                        if currency_name is not None and currency_name:
                            first_id: int = self.get_currency_id(currency_name)

                            found_currency_pairs = session.query(ExchangeCurrencyPair).filter(
                                ExchangeCurrencyPair.exchange_id.__eq__(exchange_id),
                                ExchangeCurrencyPair.first_id.__eq__(first_id)).all()

                            if found_currency_pairs is not None:
                                all_found_currency_pairs.extend(found_currency_pairs)
                session.expunge_all()
        return all_found_currency_pairs

    def get_currency_pairs_with_second_currency(self, exchange_name: str, currency_names: str) \
            -> List[ExchangeCurrencyPair]:
        """
        Returns all currency-pairs for the given exchange that have any of the given currencies
        as the second currency.

        @param exchange_name: str
            Name of the exchange.
        @param currency_names: List[str]
            List of the currency names that are viable as second currencies.
        @return:
            List of the currency-pairs which end with any of the currencies in currency_names
            on the given exchange.
            List is empty if there are no currency pairs in the database which fulfill the requirements.
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
                                ExchangeCurrencyPair.exchange_id.__eq__(exchange_id),
                                ExchangeCurrencyPair.second_id.__eq__(second_id)).all()

                            if found_currency_pairs is not None:
                                all_found_currency_pairs.extend(found_currency_pairs)

                session.expunge_all()
        return all_found_currency_pairs

    def get_currency_pairs(self, exchange_name: str, currency_pairs: List[Dict[str, str]]) \
            -> List[ExchangeCurrencyPair]:
        """
        Returns all ExchangeCurrencyPairs for the given exchange if they fit any
        currency pairs in the given list of dictionaries.

        @param exchange_name: str
            Name of the exchange.
        @param currency_pairs: str
            List of the currency pairs that should be found.
            Each dictionary should contain the keys 'first' and 'second'
            which contain the names of the currencies.
        @return:
            List of all found currency pairs on this exchange based on the given pair combinations.
        """
        found_currency_pairs: List[ExchangeCurrencyPair] = list()

        if exchange_name:
            exchange_id: int = self.get_exchange_id(exchange_name)
            with self.session_scope() as session:
                if currency_pairs is not None:
                    for currency_pair in currency_pairs:
                        first_currency = currency_pair['first']
                        second_currency = currency_pair['second']
                        if first_currency and second_currency:
                            first_id: int = self.get_currency_id(first_currency)
                            second_id: int = self.get_currency_id(second_currency)

                            found_currency_pair = session.query(ExchangeCurrencyPair).filter(
                                ExchangeCurrencyPair.exchange_id.__eq__(exchange_id),
                                ExchangeCurrencyPair.first_id.__eq__(first_id),
                                ExchangeCurrencyPair.second_id.__eq__(second_id)).first()

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
            if 'all' in currency_pairs:
                found_currency_pairs.extend(self.get_all_currency_pairs_from_exchange(exchange_name))
            elif currency_pairs[0] is not None:
                found_currency_pairs.extend(self.get_currency_pairs(exchange_name, currency_pairs))

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
            return session.query(Exchange.id).filter(Exchange.name.__eq__(exchange_name.upper())).scalar()

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
            return session.query(Currency.id).filter(Currency.name.__eq__(currency_name.upper())).scalar()

    def persist_exchange(self, exchange_name: str, is_exchange: bool):
        """
        Persists the given exchange-name if it's not already in the database.

        @param exchange_name:
            Name that should is to persist.
        @param is_exchange: boolean indicating if the exchange is indeed an exchange or a platform
        """

        with self.session_scope() as session:
            exchange_id = session.query(Exchange.id).filter(Exchange.name.__eq__(exchange_name.upper())).first()
            if exchange_id is None:
                exchange = Exchange(name=exchange_name, is_exchange=is_exchange)
                session.add(exchange)

    # ToDo: den boolean 'is_exchange' in jedes *.yaml-file schreiben.
    # ToDo: alle persist methoden überprüfen ob der boolean 'is_exchange' implementiert ist.
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
            with self.session_scope() as session:
                for cp in tqdm.tqdm(currency_pairs, disable=(len(currency_pairs) < 10)):
                    exchange_name = cp[0]
                    first_currency_name = cp[1]
                    second_currency_name = cp[2]

                    if exchange_name is None or first_currency_name is None or second_currency_name is None:
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
                        ExchangeCurrencyPair.second_id == second.id).first()

                    if existing_exchange_pair is None:
                        exchange_pair = ExchangeCurrencyPair(exchange=exchange, first=first, second=second)
                        # ex_currency_pairs.append(exchange_pair)
                        session.add(exchange_pair)

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

    # NEVER CALL THIS OUTSIDE OF THIS CLASS
    def get_exchange_currency_pair(self, session: Session, exchange_name: str, first_currency_name: str,
                                   second_currency_name: str) -> ExchangeCurrencyPair:
        """
        Checks if there is a currency pair in the database with the given parameters and
        returns it if so.

        @param session: Session
            sqlalchemy-session.
        @param exchange_name: str
            Name of the exchange.
        @param first_currency_name: str
            Name of the first currency in the currency pair.
        @param second_currency_name: str
            Name of the second currency in the currency pair.
        @return:
            The ExchangeCurrencyPair which fulfills all the requirements or None
            if no such ExchangeCurrencyPair exists.
        """

        if exchange_name is None or first_currency_name is None or second_currency_name is None:
            return None
        # sollte raus in der actual Implementierung
        self.persist_exchange_currency_pair(exchange_name, first_currency_name, second_currency_name, is_exchange=True)
        ex = session.query(Exchange).filter(Exchange.name == exchange_name.upper()).first()
        first = session.query(Currency).filter(Currency.name == first_currency_name.upper()).first()
        second = session.query(Currency).filter(Currency.name == second_currency_name.upper()).first()

        cp = session.query(ExchangeCurrencyPair).filter(ExchangeCurrencyPair.exchange.__eq__(ex),
                                                        ExchangeCurrencyPair.first.__eq__(first),
                                                        ExchangeCurrencyPair.second.__eq__(second)).first()
        return cp

    def general_persist(self, exchange_name: str, method: str, db_table, data: Iterable, mappings: List):
        """
        #ToDo: Doku schreiben
        @param exchange_name:
        @param method:
        @param db_table:
        @param data:
        @param mappings:
        """

        col_names = [key.name for key in inspect(db_table).columns]
        primary_keys = [key.name for key in inspect(db_table).primary_key]

        check_columns = [mapping in col_names for mapping in mappings]

        if not all(check_columns):
            failed_columns = dict(zip([mapping for mapping in mappings], check_columns))

            logging.error("YAML mapping-keys do not match database columns for {}: \n {}".format(method.upper(),
                                                                                                 failed_columns))
            raise ValueError("YAML mapping-keys do not match database columns for {}: \n {}".format(method.upper(),
                                                                                                    failed_columns))

        tuple_counter = 0
        with self.session_scope() as session:
            for data_tuple in data:
                data_tuple = dict(zip(mappings, data_tuple))
                p_key_filter = {key: data_tuple.get(key, None) for key in primary_keys}
                query_exists: bool = True if session.query(db_table). \
                                                 filter_by(**p_key_filter).count() > 0 else False

                if not query_exists:
                    tuple_counter += 1
                    add_tuple = db_table(**data_tuple)
                    session.add(add_tuple)

        print('{} tuple(s) added to {} for {}.'.format(tuple_counter, method, exchange_name.capitalize()))
        logging.info('{} tuple(s) added to {} for {}.'.format(tuple_counter, method, exchange_name.capitalize()))

    def get_readable_query(self,
                           db_table: object,
                           query_everything: bool,
                           from_timestamp: datetime,
                           to_timestamp: datetime,
                           exchanges: List[str],
                           currency_pairs: List[Dict[str, str]],
                           first_currencies: List[str],
                           second_currencies: List[str]):

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
            col_names = [key.name for key in inspect(db_table).columns]

            data: Query = session.query(Exchange.name.label('exchange'),
                                        first.name.label('first_currency'),
                                        second.name.label('second_currency'),
                                        db_table). \
                join(ExchangeCurrencyPair, db_table.exchange_pair_id == ExchangeCurrencyPair.id). \
                join(Exchange, ExchangeCurrencyPair.exchange_id == Exchange.id). \
                join(first, ExchangeCurrencyPair.first_id == first.id). \
                join(second, ExchangeCurrencyPair.second_id == second.id)#.options(joinedload('exchange_pair'))

            # data = data.options(undefer('*'))

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

    # Methods that are currently not used but might be useful:

    # def get_exchange_ids(self, exchange_names: List[str]) -> List[int]:
    #     exchange_ids: List[int] = list()
    #     with self.session_scope() as session:
    #         if exchange_names:
    #             exchanges = [x.upper() for x in exchange_names]
    #             with self.session_scope() as session:
    #                 exchange_ids = session.query(Exchange.id).filter(Exchange.name.in_(exchanges)).all()
    #         else:
    #             exchange_ids = session.query(Exchange.id).all()
    #
    #         return [r[0] for r in exchange_ids]
    #
    # def get_all_exchange_names(self) -> List[str]:
    #     with self.session_scope() as session:
    #         return [r[0] for r in session.query(Exchange.name).all()]



    # def get_readable_tickers(self,
    #                          query_everything: bool,
    #                          from_timestamp: datetime,
    #                          to_timestamp: datetime,
    #                          exchanges: List[str],
    #                          currency_pairs: List[Dict[str, str]],
    #                          first_currencies: List[str],
    #                          second_currencies: List[str]):
    #     """
    #     Queries based on the parameters readable ticker data and returns it.
    #     If query_everything is true, everything ticker tuple will be returned.
    #     This is also the case if query_everything is false but there were no
    #     exchanges or currencies/currency pairs given.
    #     If exchanges are given only tuples of these given exchanges will be returned.
    #     If there are no currencies/currency pairs given,
    #     all ticker-tuple of the given exchange will be returned.
    #     If currencies are given note that only ticker tuple with currency pairs,
    #     which have either any currency in first_currencies as first OR any currency
    #     in second_currencies as second OR any currency pairs in currency_pairs will be returned.
    #     If timestamps are given the queried tuples will be filtered accordingly.
    #
    #     So query logic for each tuple is (if exchange, currencies and time are given):
    #         exchange AND (first OR second OR pair) AND from_time AND to_time
    #
    #     See csv-config for details of how to write/give parameters.
    #     @param query_everything: bool
    #         If everything in the database should be queried.
    #     @param from_timestamp: datetime
    #         Minimum date for the start of the request.
    #     @param to_timestamp: datetime
    #         Maximum date for the start of the request.
    #     @param exchanges: List[str]
    #         List of exchanges of which the tuple should be queried.
    #     @param currency_pairs: List[Dict[str, str]]
    #         List of specific currency pairs that should be queried.
    #         Dict needs to have the following structure:
    #             - first: 'Name of the first currency'
    #               second: 'Name of the second currency'
    #     @param first_currencies: List[str]
    #         List of viable currencies for the first currency in a currency pair.
    #     @param second_currencies: List[str]
    #         List of viable currencies for the second currency in a currency pair.
    #     @return:
    #         List of readable ticker tuple.
    #         List might be empty if database is empty or there where no ExchangeCurrencyPairs
    #         which fulfill the above stated requirements.
    #     """
    #
    #     with self.session_scope() as session:
    #         first = aliased(Currency)
    #         second = aliased(Currency)
    #         data: Query = session.query(Exchange.name.label('exchange'),
    #                                     first.name.label('first_currency'),
    #                                     second.name.label('second_currency'),
    #                                     Ticker.start_time,
    #                                     Ticker.time,
    #                                     Ticker.last_price,
    #                                     Ticker.best_ask,
    #                                     Ticker.best_bid,
    #                                     Ticker.daily_volume). \
    #             join(ExchangeCurrencyPair, Ticker.exchange_pair_id == ExchangeCurrencyPair.id). \
    #             join(Exchange, ExchangeCurrencyPair.exchange_id == Exchange.id). \
    #             join(first, ExchangeCurrencyPair.first_id == first.id). \
    #             join(second, ExchangeCurrencyPair.second_id == second.id)
    #
    #         if query_everything:
    #             result = data.all()
    #         else:
    #             exchange_names = list()
    #             first_currency_names = list()
    #             second_currency_names = list()
    #             currency_pairs_names = list()
    #
    #             if exchanges:
    #                 exchange_names = [name.upper() for name in exchanges]
    #             else:
    #                 exchange_names = [r[0] for r in session.query(Exchange.name)]
    #             if not first_currencies and not second_currencies and not currency_pairs:
    #                 first_currency_names = [r[0] for r in session.query(Currency.name)]
    #             else:
    #                 if first_currencies:
    #                     first_currency_names = [name.upper() for name in first_currencies]
    #                 if second_currencies:
    #                     second_currency_names = [name.upper() for name in second_currencies]
    #                 if currency_pairs:
    #                     currency_pairs_names = [(pair['first'].upper(), pair['second'].upper()) for pair in
    #                                             currency_pairs]
    #
    #             result = data.filter(and_(
    #                 Exchange.name.in_(exchange_names),
    #                 or_(
    #                     first.name.in_(first_currency_names),  # first currency
    #                     second.name.in_(second_currency_names),  # second currency
    #                     tuple_(first.name, second.name).in_(currency_pairs_names)  # currency_pair
    #                 ),
    #             ))
    #
    #             if from_timestamp:
    #                 result = result.filter(Ticker.start_time >= from_timestamp)
    #             if to_timestamp:
    #                 result = result.filter(Ticker.start_time <= to_timestamp)
    #
    #             result = result.all()
    #     return result

