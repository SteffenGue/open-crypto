from datetime import datetime
from typing import Sequence, List, Tuple, Any, Iterator, Iterable
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy import create_engine, MetaData, or_, and_, exists
from sqlalchemy.orm import sessionmaker, Session, Load, joinedload, raiseload, selectinload, subqueryload, eagerload, \
    defer
from tables import Currency, Exchange, ExchangeCurrencyPair, Ticker, HistoricRate


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

        :param metadata: Metadata
            Information about the table-structure of the database.
            See tables.py for more information.
        :param sqltype: atr
            Type of the database sql-dialect. ('postgresql' for us)
        :param client: str
            Name of the Client which is used to connect to the database.
        :param user_name: str
            Username under which this program connects to the database.
        :param password: str
            Password for this username.
        :param host: str
            Hostname or Hostaddress from the database.
        :param port: str
            Connection-Port (usually 5432 for Postgres)
        :param db_name: str
            Name of the database.
        """

        engine = create_engine(
            '{}+{}://{}:{}@{}:{}/{}'.format(sqltype, client, user_name, password, host, port, db_name))

        if not database_exists(engine.url):
            create_database(engine.url)
            print(f"Database '{db_name}' created")
        metadata.create_all(engine)
        self.sessionFactory = sessionmaker(bind=engine)

    # ToDo: Load all DB-Entries once in the beginning instead of querying every item speratly?!

    def persist_tickers(self,
                        tickers: Iterator[Tuple[str, datetime, datetime, str, str, float, float, float, float, float]]):
        """
        Persists the given tuples of ticker-data.
        TUPLES MUST HAVE THE DESCRIBED STRUCTURE STATED BELOW

        The method checks for each tuple if the referenced exchange and
        currencies exist in the database.
        If so, the Method creates with the stored data of the current tuple
        a new Ticker-object which is then added to the commit.
        After all tuples where checked, the added Ticker-objects will be
        committed and the connection will be closed.

        Exceptions will be caught but not really handled.
        TODO: Exception handling and
        TODO: Logging of Exception

        :param tickers: Iterator
            Iterator of tuples containing ticker-data.
            Tuple must have the following structure:
                (exchange-name,
                 start_time,
                 response_time,
                 first_currency_symbol,
                 second_currency_symbol,
                 ticker_last_price,
                 ticker_last_trade,
                 ticker_best_ask,
                 ticker_best_bid,
                 ticker_daily_volume)
        """
        session = self.sessionFactory()

        try:
            for ticker in tickers:
                exchange_currency_pair = self.persist_exchange_currency_pair(ticker[0], ticker[3], ticker[4])

                ticker_tuple = Ticker(exchange_pair_id=exchange_currency_pair.id,
                                      exchange_pair=exchange_currency_pair,
                                      start_time=ticker[1],
                                      response_time=ticker[2],
                                      last_price=ticker[5],
                                      last_trade=ticker[6],
                                      best_ask=ticker[7],
                                      best_bid=ticker[8],
                                      daily_volume=ticker[9])

                session.add(ticker_tuple)
            # TODO: if error commit wieder einrücken
            session.commit()
            session.close()

        except Exception as e:
            print(e, e.__cause__)
            session.rollback()
            pass
        finally:
            session.close()

    def get_active_exchanges(self):
        """
        Query every inactive exchange from the database
        :return: list of all inactive exchanges
        """

        session = self.sessionFactory()
        query = set(session.query(Exchange.name).filter(Exchange.active == False).all())
        session.close()
        return query

    def update_exceptions(self, exceptions: dict):
        """
        Method to update the exception_counter. If An exception occurred add 1 to the counter,
            else set back to zero.
        Further exchanges with a total of 3 exceptions in a row will be set inactive.

        :param exceptions: dict
            Dictionary with key (Exchange) value (boolean) pair.
        :return: None
        """

        session = self.sessionFactory()
        exchanges = list(session.query(Exchange).all())

        for exchange in exchanges:

            if exchange.name in exceptions:
                exchange.exceptions += 1
                exchange.total_exceptions += 1
                print('{}: Exception Counter +1'.format(exchange.name))

                if exchange.exceptions > 3:
                    exchange.active = False
                    print('{} was set inactive.'.format(exchange.name))
            else:
                exchange.exceptions = 0

        try:
            session.commit()

        except Exception as e:
            print(e, e.__cause__)
            session.rollback()
            pass

        finally:
            session.close()

    def request_params(self, function, exchange, *args: list):
        """
        Helper-method to perform function calls from request parameters. If one needs a variable
        request parameter (for example: the latest entry of a specific currency pair in the database),
        a lambda function can be defined in utilities.py and is called in exchanges.py as the parameters
        from the yaml-files are read out. The specifications when a function call takes place are described
        in exchanges.py under the method Exchanges.extract_request_urls().

        :param function: dict
            {'function': <function>, 'params': int, 'session': boolean}

        :param exchange: str
            the actual exchange name

        :param args: *args - variable number of parameters according to the function

        :return the desired unassigned value. Can be string/float/integer.
        """

        if function['params'] == len(args):
            session = self.sessionFactory()

            func = function['function']

            if function['session'] is True:
                # evaluate the *args to rewrite them as the objects they should represent.
                args = (session, eval(*args))

            session.close()
            return func(*args).first()

        else:
            raise KeyError(f'Wrong number of arguments for {exchange} - {function["name"]}. '
                           f'Expected {function["params"]} - got {len(args)}')

    # TODO: Dokumentation, Möglichkeit nur einzelne currency_pairs zu bekommen
    def get_all_exchange_currency_pairs(self, exchange_name: str) -> List[ExchangeCurrencyPair]:
        """
        @param exchange_name:
            Name of the exchange that the currency-pairs should be queried for.
        @return:
            List of all currency-pairs for the given exchange.
        """
        session = self.sessionFactory()
        currency_pairs = list()
        exchange_id = session.query(Exchange.id).filter(Exchange.name.__eq__(exchange_name.upper())).first()
        if exchange_id is not None:
            currency_pairs = session.query(ExchangeCurrencyPair).filter(
                # ExchangeCurrencyPair.exchange_id.__eq__(exchange_id)).all()
                ExchangeCurrencyPair.exchange_id.__eq__(exchange_id),
                ExchangeCurrencyPair.second_id.__eq__(6)).all() #WICHTIG DEN FILTER RAUSZUNEHMEN
        session.close()
        return currency_pairs

    def persist_exchange(self, exchange_name: str):
        """
        Persists the given exchange-name if it's not already in the database.

        @param exchange_name:
            Name that should is to persist.
        """
        session = self.sessionFactory()
        exchange_id = session.query(Exchange.id).filter(Exchange.name.__eq__(exchange_name.upper())).first()
        if exchange_id is None:
            exchange = Exchange(name=exchange_name)
            session.add(exchange)
            session.commit()
        session.close()

    def persist_exchange_currency_pairs(self, currency_pairs: Iterable[Tuple[str, str, str]]):
        """
        Persists the given already formatted ExchangeCurrencyPair-tuple if they not already exist.
        The formatting ist done in @see{Exchange.format_currency_pairs()}.

        Tuple needs to have the following structure:
            (exchange-name, first currency-name, second currency-name)

        @param currency_pairs:
            Iterator of currency-pair tuple that are to persist.
        """
        if currency_pairs is not None:
            session = self.sessionFactory()
            ex_currency_pairs: List[ExchangeCurrencyPair] = list()

            try:
                for cp in currency_pairs:
                    exchange_name = cp[0]
                    first_currency_name = cp[1]
                    second_currency_name = cp[2]

                    if exchange_name is None or first_currency_name is None or second_currency_name is None:
                        continue

                    existing_exchange = session.query(Exchange).filter(Exchange.name == exchange_name.upper()).first()
                    exchange = existing_exchange if existing_exchange is not None else Exchange(name=exchange_name)

                    existing_first_cp = session.query(Currency).filter(Currency.name == first_currency_name.upper()).first()
                    first = existing_first_cp if existing_first_cp is not None else Currency(name=first_currency_name)

                    existing_second_cp = session.query(Currency).filter(Currency.name == second_currency_name.upper()).first()
                    second = existing_second_cp if existing_second_cp is not None else Currency(name=second_currency_name)

                    existing_exchange_pair = session.query(ExchangeCurrencyPair).filter(
                        ExchangeCurrencyPair.exchange_id == exchange.id,
                        ExchangeCurrencyPair.first_id == first.id,
                        ExchangeCurrencyPair.second_id == second.id).first()

                    if existing_exchange_pair is None:
                        exchange_pair = ExchangeCurrencyPair(exchange=exchange, first=first, second=second)
                        ex_currency_pairs.append(exchange_pair)
                        session.add(exchange_pair)

                session.commit()
                print('{} Currency Pairs für {} hinzugefügt'.format(ex_currency_pairs.__len__(), exchange_name))
            except Exception as e:
                print(e, e.__cause__)
                session.rollback()
                pass
            finally:
                session.close()

    def persist_exchange_currency_pair(self, exchange_name: str, first_currency_name: str,
                                       second_currency_name: str) -> ExchangeCurrencyPair:
        """
        Adds a single ExchangeCurrencyPair to the database is it does not already exist.

        @param exchange_name:
            Name of the exchange.
        @param first_currency_name:
            Name of the first currency.
        @param second_currency_name:
            Name of the second currency.
        """
        self.persist_exchange_currency_pairs((exchange_name, first_currency_name, second_currency_name))

    def persist_historic_rates(self, historic_rates: Iterable[Tuple[int, datetime, float, float, float, float, float]]):
        """
        Persists the given already formatted historic-rates-tuple if they not already exist.
        The formatting ist done in @see{Exchange.format_historic_rates()}.

        @param historic_rates:
            Iterator containing the already formatted historic-rates-tuple.
        """
        session = self.sessionFactory()
        try:
            i = 0
            for historic_rate in historic_rates:
                tuple_exists = session.query(HistoricRate.exchange_pair_id). \
                    filter(
                    HistoricRate.exchange_pair_id == historic_rate[0],
                    HistoricRate.timestamp == historic_rate[1]
                ). \
                    first()
                if tuple_exists is None:
                    i += 1
                    hr_tuple = HistoricRate(exchange_pair_id=historic_rate[0],
                                            timestamp=historic_rate[1],
                                            open=historic_rate[2],
                                            high=historic_rate[3],
                                            low=historic_rate[4],
                                            close=historic_rate[5],
                                            volume=historic_rate[6])
                    session.add(hr_tuple)
            session.commit()
            session.close()
            print('{} tupel eingefügt in historic rates.'.format(i))
        except Exception as e:
            print(e, e.__cause__)
            session.rollback()
            pass
        finally:
            session.close()
