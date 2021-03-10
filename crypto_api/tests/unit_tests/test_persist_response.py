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
        """
        Test for the method get_all_currency_pairs_from_exchange. This method will be called with the testdataset. The
        list of the id's of the exchange, first and second currency will be compared. All currencypairs from the
        testdataset should be returned of the method call, because all pairs are from the given exchange 'TESTEXCHANGE'.
        """
        test_result = self.db_handler.get_all_currency_pairs_from_exchange('TESTEXCHANGE')
        test_result = [(item.exchange_id,
                        item.first_id,
                        item.second_id) for item in test_result]
        result = self.session.query(ExchangeCurrencyPair).all()
        result = [(item.exchange_id,
                   item.first_id,
                   item.second_id) for item in result]
        self.assertEqual(result, test_result)

    def test_get_all_currency_pairs_from_exchange_with_invalid_pair(self):
        """
        Test for the method get_all_currency_pairs_from_exchange. This method will be called with the testdataset. An
        empty list of currencypairs should be returned, because in the given dataset of this testmethod are no
        currencypairs with the given exchange 'TESTEXCHANGE'.
        """
        self.session.query(ExchangeCurrencyPair).delete()
        self.db_handler.persist_exchange_currency_pair('invalid', 'BTC', 'ETH', True)
        test_result = self.db_handler.get_all_currency_pairs_from_exchange('TESTEXCHANGE')
        result = []
        self.assertEqual(result, test_result)

        self.session.query(ExchangeCurrencyPair).delete()
        self.db_handler.persist_exchange_currency_pairs(self.exchange_currency_pairs,
                                                        is_exchange=True)

    def test_get_currency_pairs_with_first_currency_valid_1(self):
        """
        Test for the method get_currency_with_first_currency. This method will be called with the testdataset and 'BTC'
        as a currency. The list of the id's of the first currency will be compared.
        """
        test_result = self.db_handler.get_currency_pairs_with_first_currency('TESTEXCHANGE', ['BTC'])
        test_result = [item.first_id for item in test_result]
        result = self.session.query(ExchangeCurrencyPair).filter(ExchangeCurrencyPair.first_id.__eq__(1)).all()
        result = [item.first_id for item in result]
        self.assertEqual(result, test_result)

    def test_get_currency_pairs_with_first_currency_valid_2(self):
        """
        Test for the method get_currency_with_first_currency. This method will be called with the testdataset and 'BTC'
        and 'LTC' as a currency. The list of the id's of the first currency will be compared.
        """
        test_result = self.db_handler.get_currency_pairs_with_first_currency('TESTEXCHANGE', ['BTC', 'LTC'])
        test_result = [item.first_id for item in test_result]
        result = self.session.query(ExchangeCurrencyPair).filter(ExchangeCurrencyPair.first_id.__eq__(1)).all()
        result.extend(self.session.query(ExchangeCurrencyPair).filter(ExchangeCurrencyPair.first_id.__eq__(3)).all())
        result = [item.first_id for item in result]
        self.assertEqual(result, test_result)

    def test_get_currency_pairs_with_first_currency_invalid(self):
        """
        Test for the method get_currency_with_first_currency. This method will be called with the testdataset and 'BTC'
        as a currency. The list of the id's of the first currency will be compared. An Empty list should be returned,
        because there are no currencypairs with the currency 'BAT'.
        """
        test_result = self.db_handler.get_currency_pairs_with_first_currency('TESTEXCHANGE', ['BAT'])
        result = []
        self.assertEqual(result, test_result)

    def test_get_currency_pairs_with_second_currency_valid(self):
        """
        Test for the method get_currency_with_second_currency. This method will be called with the testdataset and 'BTC'
        as a currency. The List of the id's of the second currency will be compared.
        """
        #todo : Eingabeparameter in der Methode get_currency_pairs_with_second_currency in db_handler
        #       müsste eigentlich eine Liste an Currencies entgegennehmen, wie in der Methode
        #       get_currency_pairs_with_first_currency, und nicht nur einen einzelnen String.
        # Ich habe das noch nicht gefixt, da ich nicht genau weiß, ob dann eventuell Fehlermeldungen geworfen werden, bei den vorhandenen Aufrufen der Methode. Diese Aufrufe müssten dann eventuell angepasst werden.
        test_result = self.db_handler.get_currency_pairs_with_second_currency('TESTEXCHANGE', ['BTC'])
        test_result = [item.second_id for item in test_result]
        result = self.session.query(ExchangeCurrencyPair).filter(ExchangeCurrencyPair.second_id.__eq__(1)).all()
        result = [item.second_id for item in result]
        self.assertEqual(result, test_result)

    def test_get_currency_pairs_with_second_currency_invalid(self):
        """
        Test for the method get_currency_with_second_currency. This method will be called with the testdataset and 'BTC'
        as a currency. The list of the id's of the second currency will be compared. An Empty list should be returned,
        because there are no currencypairs with the currency 'BAT'.
        """
        test_result = self.db_handler.get_currency_pairs_with_second_currency('TESTEXCHANGE', ['BAT'])
        result = []
        self.assertEqual(result, test_result)

    def test_persist_exchange_and_get_exchange_id(self):
        """
        Test for the methods persist_exchange and get_exchange_id. The method persist_exchange will be called to persist
        a new test exchange. The return of the method get_exchange_id will be compared. Afterwards the new testexchange
        will be deleted from the testdataset.
        """
        self.db_handler.persist_exchange('TEST', True)
        test_result = self.db_handler.get_exchange_id('TEST')
        result = self.session.query(Exchange).all()
        for item in result:
            if item.name == 'TEST':
                result_id = item.id
        self.assertEqual(result_id, test_result)

        self.session.query(Exchange).filter(Exchange.id.__eq__(result_id)).delete()

    def test_get_currency_id(self):
        """
        Test for the method get_currency_id. This method will be called. The returned id's will be compared.
        """
        test_result = self.db_handler.get_currency_id('BTC')
        result = self.session.query(Currency).all()
        for item in result:
            if item.name == 'BTC':
                result_id = item.id
        self.assertEqual(result_id, test_result)

    def test_get_currency_pairs(self):
        """
        Test for the method get_currency_pairs. This method will be called with a given testexchange and a given list of
        dictionaries (representing currency pairs).
        For simplicity, only the id's will be compared (not the whole objects).
        """
        currency_pairs = [{'first': 'BTC', 'second': 'LTC'},
                          {'first': 'BTC', 'second': 'DIO'}]
        test_result = self.db_handler.get_currency_pairs('TESTEXCHANGE', currency_pairs)
        test_result = [(item.exchange_id,
                        item.first_id,
                        item.second_id) for item in test_result]
        result = self.session.query(ExchangeCurrencyPair).filter(ExchangeCurrencyPair.exchange_id.__eq__(1),
                                                                 ExchangeCurrencyPair.first_id.__eq__(1),
                                                                 ExchangeCurrencyPair.second_id.__eq__(3)).all()
        result.extend(self.session.query(ExchangeCurrencyPair).filter(ExchangeCurrencyPair.exchange_id.__eq__(1),
                                                                      ExchangeCurrencyPair.first_id.__eq__(1),
                                                                      ExchangeCurrencyPair.second_id.__eq__(5)).all())
        result = [(item.exchange_id,
                   item.first_id,
                   item.second_id) for item in result]
        self.assertEqual(result, test_result)

    def test_get_exchange_currency_pairs1(self):
        """
        Test for the method get_exchange_currency_pairs. This method will be called with a given testexchange, list of
        dictionaries (representing currency pairs), al ist of first currencies and a list of second currencies.
        For simplicity, only the id's will be compared (not the whole objects).
        """
        currency_pairs = [{'first': 'BTC', 'second': 'LTC'}]
        firsts = ['DIO']
        seconds = []
        test_result = self.db_handler.get_exchanges_currency_pairs('TESTEXCHANGE', currency_pairs, firsts, seconds)
        test_result = [(item.exchange_id,
                        item.first_id,
                        item.second_id) for item in test_result]
        result = self.session.query(ExchangeCurrencyPair).filter(ExchangeCurrencyPair.exchange_id.__eq__(1),
                                                                 ExchangeCurrencyPair.first_id.__eq__(1),
                                                                 ExchangeCurrencyPair.second_id.__eq__(3)).all()
        result.extend(self.session.query(ExchangeCurrencyPair).filter(ExchangeCurrencyPair.exchange_id.__eq__(1),
                                                                      ExchangeCurrencyPair.first_id.__eq__(5)).all())
        result = [(item.exchange_id,
                   item.first_id,
                   item.second_id) for item in result]
        self.assertEqual(result, test_result)

    def test_get_exchange_currency_pairs2(self):
        """
        Test for the method get_exchange_currency_pairs. This method will be called with a given testexchange, list of
        dictionaries (representing currency pairs), al ist of first currencies and a list of second currencies.
        For simplicity, only the id's will be compared (not the whole objects).
        """
        currency_pairs = [{'first': 'BTC', 'second': 'DASH'}]
        firsts = ['XRP']
        seconds = ['ETH']
        test_result = self.db_handler.get_exchanges_currency_pairs('TESTEXCHANGE', currency_pairs, firsts, seconds)
        test_result = [(item.exchange_id,
                        item.first_id,
                        item.second_id) for item in test_result]
        result = self.session.query(ExchangeCurrencyPair).filter(ExchangeCurrencyPair.exchange_id.__eq__(1),
                                                                 ExchangeCurrencyPair.first_id.__eq__(1),
                                                                 ExchangeCurrencyPair.second_id.__eq__(6)).all()
        result.extend(self.session.query(ExchangeCurrencyPair).filter(ExchangeCurrencyPair.exchange_id.__eq__(1),
                                                                      ExchangeCurrencyPair.first_id.__eq__(4),
                                                                      ExchangeCurrencyPair.second_id.__eq__(2)).all())

        result = [(item.exchange_id,
                   item.first_id,
                   item.second_id) for item in result]
        result = list(dict.fromkeys(result))  # remove duplicates from list
        self.assertEqual(result, test_result)
