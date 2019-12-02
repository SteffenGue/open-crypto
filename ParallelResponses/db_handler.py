from typing import Sequence, List, Tuple, Any, Iterator

from sqlalchemy import create_engine, MetaData, or_, and_
from sqlalchemy.orm import sessionmaker
from tables import Currency, CurrencyPair, Exchange, Ticker


class DatabaseHandler:

    # TODO guten Primary Key finden, die Connextions öffnen und closen, mehr struktur.
    # def __init__(self):
    #     self.connection_params = read_config('database')
    #     # cursor = self.connect().cursor()
    #     self.checkTables()

    def __init__(self,
                 metadata: MetaData,
                 client: str,
                 user_name: str,
                 password: str,
                 host: str,
                 port: str,
                 db_name: str):

        self.metadata = metadata
        self.engine = create_engine('{}://{}:{}@{}:{}/{}'.format(client, user_name, password, host, port, db_name))

        self.metadata.create_all(self.engine)

        self.sessionFactory = sessionmaker(bind=self.engine)

    def persist_currencies(self, currencies: Sequence[tuple]):
        class_ = Currency
        session = self.sessionFactory()
        generate_pairs = list()

        for curr in currencies:
            curr_name = curr[0]
            curr_symbol = curr[1]
            currency = Currency(name=curr_name, symbol=curr_symbol)
            # ask if currency existsts in database
            if session.query(class_). \
                    filter(class_.name == currency.name). \
                    filter(class_.symbol == currency.symbol).first() is None:
                session.add(currency)
                # generate_pairs.append(currency.id)
        session.commit()
        session.close()

        # TODO Kann wahrscheinlich Weg, da zu lange
        # self.persist_currency_pairs(self.generate_currency_pairs(generate_pairs))

    def persist_currency_pairs(self, currency_pairs: Sequence[tuple]):
        session = self.sessionFactory()

        to_add = len(currency_pairs)
        counter = 0

        for pair in currency_pairs:
            counter = counter + 1
            currency_pair = CurrencyPair(first_id=pair[0], second_id=pair[1])
            if session.query(CurrencyPair). \
                    filter(or_(and_(CurrencyPair.first_id == currency_pair.first_id,
                                    CurrencyPair.second_id == currency_pair.second_id),
                               and_(CurrencyPair.first_id == currency_pair.second_id,
                                    CurrencyPair.second_id == currency_pair.first_id))).first() is None:
                session.add(currency_pair)
                print('Paar: {}, {} hinzugefügt.'.format(currency_pair.first_id, currency_pair.second_id))

            print('{} von {}'.format(counter, to_add))
        session.commit()
        print('index ist lit')
        session.close()

    def get_all_currency_ids(self) -> Sequence[int]:
        session = self.sessionFactory()
        tuple_ids = session.query(Currency.id).all()
        session.close()
        list_ids = [value for value, in tuple_ids]
        return list_ids

    def generate_currency_pairs(self, ids: Sequence[int]):
        pairs = list()

        persisted_ids = self.get_all_currency_ids()
        for id in ids:
            for p_id in persisted_ids:
                if not id == p_id:
                    pairs.append((id, p_id))
                    print('{}, {}'.format(id, p_id))

        return pairs

    def bulk_currency_pairs(self, pairs: Sequence[tuple]):
        currency_pairs = list()
        for p in pairs:
            currency_pairs.append(CurrencyPair(first_id=p[0], second_id=p[1]))

        session = self.sessionFactory()
        session.bulk_save_objects(currency_pairs)
        session.commit()
        print('done')
        session.close()

    #   name
    #   time
    #   'currency_pair_first'
    #   'currency_pair_second'
    #   'ticker_last_price'
    #   'ticker_last_trade'
    #   'ticker_best_ask'
    #   'ticker_best_bid'
    #   'ticker_daily_volume'
    def persist_tickers(self, tickers: Iterator):
        session = self.sessionFactory()
        for ticker in tickers:
            print(ticker)
            if ticker[2] and ticker[3] and self.currency_exists(ticker[2]) and self.currency_exists(ticker[3]):
                ticker_tuple = Ticker(exchange=ticker[0],
                                      time=ticker[1],
                                      first_currency=ticker[2],
                                      second_currency=ticker[3],
                                      last_price=ticker[4],
                                      last_trade=ticker[5],
                                      best_ask=ticker[6],
                                      best_bid=ticker[7],
                                      daily_volume=ticker[8])
                session.add(ticker_tuple)
            else:
                print('Currency {} oder {} nicht in der Datenbank gefunden.'.format(ticker[2], ticker[3]))
        try:
            session.commit()
        except Exception:
            print('Exception beim persistieren.')

        session.close()

    def get_exchange_id(self, exchange_name: str) -> int:
        session = self.sessionFactory()
        result = session.query(Exchange.id). \
            filter(Exchange.name == exchange_name).first()
        session.close()
        return result

    def get_currency_pair_id(self, first_currency_name: str, second_currency_name: str) -> int:
        session = self.sessionFactory()

        first_currency_id = session.query(Currency.id). \
            filter(Currency.symbol == first_currency_name.upper()). \
            first()

        second_currency_id = session.query(Currency.id). \
            filter(Currency.symbol == second_currency_name.upper()). \
            first()

        if first_currency_id is not None and second_currency_id is not None:
            # Value aus Tupel holen
            first_currency_id = first_currency_id[0]
            second_currency_id = second_currency_id[0]

            result = session.query(CurrencyPair.id). \
                filter(or_(and_(CurrencyPair.first_id == first_currency_id,
                                CurrencyPair.second_id == second_currency_id),
                           and_(CurrencyPair.first_id == second_currency_id,
                                CurrencyPair.second_id == first_currency_id))). \
                first()
        else:
            result = None

        session.close()

        # Value aus Tuple holen, der returnt wird
        if result is not None:
            result = result[0]
        return result

    def persist_exchanges(self, exchanges: list):
        session = self.sessionFactory()
        for exchange_name in exchanges:
            if session.query(Exchange.name). \
                    filter(Exchange.name == exchange_name). \
                    first() is None:
                session.add(Exchange(name=exchange_name))
        session.commit()
        session.close()

    def currency_exists(self, curr_symbol: str) -> bool:
        session = self.sessionFactory()

        result = session.query(Currency). \
            filter(Currency.symbol == curr_symbol.upper()). \
            first()
        session.close()
        return result is not None
