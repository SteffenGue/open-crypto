import unittest
from model.database.db_handler import DatabaseHandler
from model.database.tables import *
from itertools import permutations
from datetime import datetime
from model.utilities.exceptions import NotAllPrimaryKeysException


class TestPersistResponse(unittest.TestCase):
    """Test class for DatabaseHandler."""

    db_config = {
        'sqltype': None,
        'client': None,
        'user_name': None,
        'password': None,
        'host': None,
        'port': None,
        'db_name': None,
    }

    db_handler = DatabaseHandler(metadata, debug=True, **db_config)
    session = db_handler.sessionFactory()
    currencies = ['BTC', 'ETH', 'LTC', 'XRP', 'DIO', 'DASH']
    currency_pairs = permutations(currencies, 2)
    exchange_currency_pairs = [('TESTEXCHANGE',) + pair for pair in currency_pairs]
    db_handler.persist_exchange_currency_pairs(exchange_currency_pairs,
                                               is_exchange=True)

    def test_persist_exchange_currency_pairs(self):
        result = self.session.query(ExchangeCurrencyPairView).all()
        result = [(item.exchange_name, item.first_name, item.second_name) for item in result]
        self.assertEqual(self.exchange_currency_pairs, result)

    def test_persist_valid_ticker(self):
        response = [
            (datetime.utcnow(), datetime.utcnow(), 1.0, 1.0, 1.0, 1.0, 1),
            (datetime.utcnow(), datetime.utcnow(), 2.0, 2.0, 2.0, 2.0, 2),
            (datetime.utcnow(), datetime.utcnow(), 3.0, 3.0, 3.0, 3.0, 3),
            (datetime.utcnow(), datetime.utcnow(), 4.0, 4.0, 4.0, 4.0, 4), ]
        exchanges_with_pairs = {self.session.query(Exchange).first():
                                    list(self.session.query(ExchangeCurrencyPair).limit(4))}

        exchange = list(exchanges_with_pairs.keys())[0]
        request_name = Ticker.__tablename__
        mappings = ['start_time', 'time', 'best_ask', 'best_bid', 'last_price', 'daily_volume', 'exchange_pair_id']

        self.db_handler.persist_response(exchanges_with_pairs,
                                         exchange,
                                         Ticker,
                                         response,
                                         mappings)
        result = self.session.query(Ticker).all()

        result = [(item.exchange_pair.exchange.name,
                   item.exchange_pair.first.name,
                   item.exchange_pair.second.name,
                   item.start_time,
                   item.time,
                   item.exchange_pair_id,
                   item.last_price,
                   item.best_ask,
                   item.best_bid,
                   item.daily_volume) for item in result]

        response2 = [(ExCuPair[0], ExCuPair[1], ExCuPair[2]) + res for (ExCuPair, res) in
                     zip(self.exchange_currency_pairs, response)]
        self.assertEqual(response2, result)
        self.session.query(Ticker).delete()

    def test_persist_ticker_with_no_pk(self):
        response = [
            (datetime.utcnow(), 1.0, 1.0, 1.0, 1.0, 1.0), ]
        exchanges_with_pairs = {self.session.query(Exchange).first():
                                    list(self.session.query(ExchangeCurrencyPair).limit(4))}

        exchange = list(exchanges_with_pairs.keys())[0]
        request_name = Ticker.__tablename__
        mappings = ['start_time', 'best_ask', 'best_bid', 'last_price', 'daily_volume', 'exchange_pair_id']

        self.assertRaises(NotAllPrimaryKeysException, self.db_handler.persist_response, exchanges_with_pairs,
                          exchange, Ticker, response, mappings)
        self.session.query(Ticker).delete()

    def test_persist_response_with_unknown_column(self):
        response = [
            (datetime.utcnow(), datetime.utcnow(), 1.0, 1.0, 1.0, 1.0, 1, None),
            (datetime.utcnow(), datetime.utcnow(), 2.0, 2.0, 2.0, 2.0, 2, None),
            (datetime.utcnow(), datetime.utcnow(), 3.0, 3.0, 3.0, 3.0, 3, None),
            (datetime.utcnow(), datetime.utcnow(), 4.0, 4.0, 4.0, 4.0, 4, None), ]
        exchanges_with_pairs = {self.session.query(Exchange).first():
                                    list(self.session.query(ExchangeCurrencyPair).limit(4))}

        exchange = list(exchanges_with_pairs.keys())[0]
        request_name = Ticker.__tablename__
        mappings = ['start_time', 'time', 'best_ask', 'best_bid', 'last_price', 'daily_volume', 'exchange_pair_id',
                    'some_column']

        self.db_handler.persist_response(exchanges_with_pairs,
                                         exchange,
                                         Ticker,
                                         response,
                                         mappings)
        result = self.session.query(Ticker).all()
        result = [(item.exchange_pair.exchange.name,
                   item.exchange_pair.first.name,
                   item.exchange_pair.second.name,
                   item.start_time,
                   item.time,
                   item.best_ask,
                   item.best_bid,
                   item.last_price,
                   item.daily_volume,
                   item.exchange_pair_id,) for item in result]

        response2 = [(ExCuPair[0], ExCuPair[1], ExCuPair[2]) + res[:-1] for (ExCuPair, res) in
                     zip(self.exchange_currency_pairs, response)]
        self.assertEqual(response2, result)
        self.session.query(Ticker).delete()

    def test_persist_response_with_unknown_currency_pair(self):
        response = [
            ("TEST1", "TEST2", datetime.utcnow(), datetime.utcnow(), 1.0, 1.0, 1.0, 1.0, 1)]
        exchanges_with_pairs = {self.session.query(Exchange).first(): []}

        exchange = list(exchanges_with_pairs.keys())[0]
        request_name = 'Ticker'
        mappings = ['currency_pair_first', 'currency_pair_second', 'start_time', 'time', 'best_ask', 'best_bid',
                    'last_price', 'daily_volume']
        self.db_handler.persist_response(exchanges_with_pairs,
                                         exchange,
                                         Ticker,
                                         response,
                                         mappings)

        result = self.session.query(ExchangeCurrencyPairView).filter(ExchangeCurrencyPairView.first_name == "TEST1",
                                                                     ExchangeCurrencyPairView.second_name == "TEST2").first()
        self.assertEqual(("TESTEXCHANGE", "TEST1", "TEST2"), (result.exchange_name,
                                                              result.first_name,
                                                              result.second_name))
        self.session.query(Ticker).delete()

    def test_persist_response_with_none(self):
        response = [
            (datetime.utcnow(), datetime.utcnow(), None, 1.0, 1.0, 1.0, 1),
            (datetime.utcnow(), datetime.utcnow(), 2.0, None, 2.0, 2.0, 2),
            (datetime.utcnow(), datetime.utcnow(), 3.0, 3.0, None, 3.0, 3),
            (datetime.utcnow(), datetime.utcnow(), 4.0, 4.0, 4.0, None, 4), ]
        exchanges_with_pairs = {self.session.query(Exchange).first():
                                    list(self.session.query(ExchangeCurrencyPair).limit(4))}

        exchange = list(exchanges_with_pairs.keys())[0]
        request_name = Ticker.__tablename__
        mappings = ['start_time', 'time', 'best_ask', 'best_bid', 'last_price', 'daily_volume', 'exchange_pair_id']

        self.db_handler.persist_response(exchanges_with_pairs,
                                         exchange,
                                         Ticker,
                                         response,
                                         mappings)
        result = self.session.query(Ticker).all()
        result = [(item.exchange_pair.exchange.name,
                   item.exchange_pair.first.name,
                   item.exchange_pair.second.name,
                   item.start_time,
                   item.time,
                   item.best_ask,
                   item.best_bid,
                   item.last_price,
                   item.daily_volume,
                   item.exchange_pair_id,) for item in result]

        response2 = [(ExCuPair[0], ExCuPair[1], ExCuPair[2]) + res for (ExCuPair, res) in
                     zip(self.exchange_currency_pairs, response)]
        self.assertEqual(response2, result)
        self.session.query(Ticker).delete()

    def test_get_all_currency_pairs_from_exchange_with_no_invalid_pair(self):
        test_result = self.db_handler.get_all_currency_pairs_from_exchange('TESTEXCHANGE')
        test_result = [(item.exchange_id,
                        item.first_id,
                        item.second_id) for item in test_result]
        result = self.session.query(ExchangeCurrencyPair).all()
        result = [(item.exchange_id,
                   item.first_id,
                   item.second_id) for item in result]
        self.assertEqual(test_result, result)

    def test_get_all_currency_pairs_from_exchange_with_invalid_pair(self):
        self.session.query(ExchangeCurrencyPair).delete()
        self.db_handler.persist_exchange_currency_pair('invalid', 'BTC', 'ETH', True)
        test_result = self.db_handler.get_all_currency_pairs_from_exchange('TESTEXCHANGE')
        result = []
        self.assertEqual(test_result, result)
        self.db_handler.persist_exchange_currency_pairs(self.exchange_currency_pairs,
                                                        is_exchange=True)

    def test_get_currency_pairs_with_first_currency_valid_1(self):
        test_result = self.db_handler.get_currency_pairs_with_first_currency('TESTEXCHANGE', ['BTC'])
        test_result = [item.first_id for item in test_result]
        result = [1, 1, 1, 1, 1]
        self.assertEqual(test_result, result)

    def test_get_currency_pairs_with_first_currency_valid_2(self):
        test_result = self.db_handler.get_currency_pairs_with_first_currency('TESTEXCHANGE', ['BTC', 'LTC'])
        test_result = [item.first_id for item in test_result]
        result = [1, 1, 1, 1, 1, 3, 3, 3, 3, 3]
        self.assertEqual(test_result, result)

    def test_get_currency_pairs_with_second_currency_valid(self):
        #todo : Eingabeparameter in der Methode get_currency_pairs_with_second_currency in db_handler
        #       müsste eigentlich eine Liste an Currencies entgegennehmen, wie in der Methode
        #       get_currency_pairs_with_first_currency, und nicht nur einen einzelnen String.
        # Ich habe das noch nicht gefixt, da ich nicht genau weiß, ob dann eventuell Fehlermeldungen geworfen, bei den vorhandenen Aufrufen der Methode. Diese Aufrufe müssten dann eventuell angepasst werden.
        test_result = self.db_handler.get_currency_pairs_with_second_currency('TESTEXCHANGE', ['BTC'])
        test_result = [item.second_id for item in test_result]
        result = [1, 1, 1, 1, 1]
        self.assertEqual(test_result, result)

    def test_get_currency_pairs_with_second_currency_invalid(self):
        test_result = self.db_handler.get_currency_pairs_with_second_currency('TESTEXCHANGE', ['BAT'])
        result = []
        self.assertEqual(test_result, result)
