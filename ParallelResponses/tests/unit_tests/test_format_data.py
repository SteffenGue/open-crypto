import unittest
from datetime import datetime
from typing import Dict, Tuple, Optional, List
from unittest.mock import Mock

from model.database.tables import ExchangeCurrencyPair
from model.exchange.Mapping import Mapping
from model.exchange.exchange import Exchange
from model.utilities.exceptions import MappingNotFoundException, DifferentExchangeContentException, \
    NoCurrencyPairProvidedException


class TestFormatData(unittest.TestCase):
    def test_response_is_none(self):
        """Testing the handling of a response with no data in it."""
        # setup
        exchange_name = 'test_exchange'
        exchange_dict: Dict = {"name": exchange_name,
                               "exchange": True,
                               "api_url": 'https://url.to.api.com',
                               "requests": []}
        exchange = Exchange(exchange_dict)
        mappings = [Mapping('first_currency', ['data'], ['int'])]
        method: str = 'ticker'
        exchange.response_mappings = {method: mappings}
        start_time = datetime.utcnow()
        time = datetime.utcnow()

        # test for ticker-data for all available currency-pairs
        response: Tuple[str, Dict[object, Optional[Dict]]] = (exchange_name, {None: None})
        result = exchange.format_data(method, response, start_time, time)
        self.assertEqual([], result[0])
        self.assertEqual(['first_currency'], result[1])

        # test for multiple currency-pairs
        first_ecp_mock = Mock(spec=ExchangeCurrencyPair)
        second_ecp_mock = Mock(spec=ExchangeCurrencyPair)
        third_ecp_mock = Mock(spec=ExchangeCurrencyPair)
        response = (exchange_name, {first_ecp_mock: None, second_ecp_mock: None, third_ecp_mock: None})
        result = exchange.format_data(method, response, start_time, time)

        result = exchange.format_data(method, response, start_time, time)
        self.assertEqual([], result[0])
        self.assertEqual(['first_currency'], result[1])

    def test_empty_response(self):
        """Testing the handling of an empty response."""
        # todo: test for pairs and for all together

        # setup
        exchange_name = 'test_exchange'
        exchange_dict: Dict = {"name": exchange_name,
                               "exchange": True,
                               "api_url": 'https://url.to.api.com',
                               "requests": []}
        exchange = Exchange(exchange_dict)
        mappings = [Mapping('first_currency', ['data'], ['int'])]
        method: str = 'ticker'
        exchange.response_mappings = {method: mappings}
        start_time = datetime.utcnow()
        time = datetime.utcnow()

        # test for ticker-data for all available currency-pairs
        response: Tuple[str, Dict[object, Optional[Dict]]] = (exchange_name, {None: {}})
        result = exchange.format_data(method, response, start_time, time)
        self.assertEqual([], result[0])
        self.assertEqual(['first_currency'], result[1])

        # test for multiple currency-pairs
        first_ecp_mock = Mock(spec=ExchangeCurrencyPair)
        second_ecp_mock = Mock(spec=ExchangeCurrencyPair)
        third_ecp_mock = Mock(spec=ExchangeCurrencyPair)
        response = (exchange_name, {first_ecp_mock: {}, second_ecp_mock: {}, third_ecp_mock: {}})

        result = exchange.format_data(method, response, start_time, time)
        self.assertEqual([], result[0])
        self.assertEqual(['first_currency'], result[1])

    def test_no_mapping_available(self):
        """Testing the case if the data is valid but there is no mapping available."""
        # Method is there but no mappings behind
        exchange_name = 'test_exchange'
        exchange_dict: Dict = {"name": exchange_name,
                               "exchange": True,
                               "api_url": 'https://url.to.api.com',
                               "requests": []}
        exchange = Exchange(exchange_dict)
        mappings = []
        method: str = 'ticker'
        exchange.response_mappings = {method: mappings}
        start_time = datetime.utcnow()
        time = datetime.utcnow()

        response: Tuple[str, Dict[object, Optional[Dict]]] = (exchange_name, {None: {'response': 'Hello'}})
        self.assertRaises(MappingNotFoundException, exchange.format_data, method, response, start_time, time)

        # No method available
        exchange.response_mappings = {}
        self.assertRaises(MappingNotFoundException, exchange.format_data, method, response, start_time, time)

    def test_response_from_diff_exchange(self):
        """Testing the case where the given response-dict is from a different exchange.
           This is detected by the given name."""
        exchange_name = 'test_exchange'
        exchange_dict: Dict = {"name": exchange_name,
                               "exchange": True,
                               "api_url": 'https://url.to.api.com',
                               "requests": []}
        exchange = Exchange(exchange_dict)
        mappings = []
        method: str = 'ticker'
        exchange.response_mappings = {method: mappings}
        start_time = datetime.utcnow()
        time = datetime.utcnow()

        response: Tuple[str, Dict[object, Optional[Dict]]] = ('other exchange', {None: {'response': 'Hello'}})
        self.assertRaises(DifferentExchangeContentException, exchange.format_data, method, response, start_time, time)

    def test_all_request_but_no_cp_first_or_second(self):
        """ Testing special case where the response contains all the available data but
            there is no currency_pair_first or currency_pair_second as name for a mapping."""
        exchange_name = 'test_exchange'
        exchange_dict: Dict = {"name": exchange_name,
                               "exchange": True,
                               "api_url": 'https://url.to.api.com',
                               "requests": []}
        exchange = Exchange(exchange_dict)

        method: str = 'ticker'
        # currency_pair_first missing
        mappings = [Mapping('currency_pair_second', ['second'], ['str']),
                    Mapping('value', ['value'], ['str', 'int'])]
        exchange.response_mappings = {method: mappings}
        start_time = datetime.utcnow()
        time = datetime.utcnow()

        # noinspection PyTypeChecker
        response: Tuple[str, Dict[object, Optional[Dict]]] = (
        exchange_name, {None: [{'first': 'btc', 'second': 'eth', 'value': 1},
                               {'first': 'eth', 'second': 'xrp', 'value': 2},
                               {'first': 'btc', 'second': 'usd', 'value': 3},
                               {'first': 'eth', 'second': 'usdt', 'value': 4}]})
        self.assertRaises(NoCurrencyPairProvidedException, exchange.format_data, method, response, start_time, time)

        # currency_pair_second missing
        mappings = [Mapping('currency_pair_first', ['first'], ['str']),
                    Mapping('value', ['value'], ['str', 'int'])]
        exchange.response_mappings = {method: mappings}
        self.assertRaises(NoCurrencyPairProvidedException, exchange.format_data, method, response, start_time, time)

        # both msising
        mappings = [Mapping('value', ['value'], ['str', 'int'])]
        exchange.response_mappings = {method: mappings}
        self.assertRaises(NoCurrencyPairProvidedException, exchange.format_data, method, response, start_time, time)

    def test_all_request_guarding_cp(self):
        """ Testing format_data for a response that coontains all the available data.
            The data for each currency pair is guarded by the formatted string from each pair."""
        # todo: currently there is no way to get to this information lol
        pass

    def test_all_request_list_dict(self):
        """ Test for a request which contains all the available data.
            The mocked response is a list of dictionaries which contain the data that
            is to be formatted."""
        exchange_name = 'test_exchange'
        exchange_dict: Dict = {"name": exchange_name,
                               "exchange": True,
                               "api_url": 'https://url.to.api.com',
                               "requests": []}
        exchange = Exchange(exchange_dict)
        mappings = [Mapping('currency_pair_first', ['first'], ['str']),
                    Mapping('currency_pair_second', ['second'], ['str']),
                    Mapping('value', ['value'], ['str', 'int'])]
        method: str = 'ticker'
        exchange.response_mappings = {method: mappings}
        start_time = datetime.utcnow()
        time = datetime.utcnow()

        # noinspection PyTypeChecker
        response: Tuple[str, Dict[object, Optional[Dict]]] = (
        exchange_name, {None: [{'first': 'btc', 'second': 'eth', 'value': 1},
                               {'first': 'eth', 'second': 'xrp', 'value': 2},
                               {'first': 'btc', 'second': 'usd', 'value': 3},
                               {'first': 'eth', 'second': 'usdt', 'value': 4}]})

        result, keys = exchange.format_data(method, response, start_time, time)

        value_list = {(start_time, time, 'BTC', 'ETH', 1),
                      (start_time, time, 'ETH', 'XRP', 2),
                      (start_time, time, 'BTC', 'USD', 3),
                      (start_time, time, 'ETH', 'USDT', 4)}
        key_list = ['start_time', 'time', 'currency_pair_first', 'currency_pair_second', 'value']

        self.assertEqual(value_list, set(result))
        self.assertEqual(key_list, keys)

    def test_all_request_list_dict_dict(self):
        """Test for a request which contains all the available data."""
        exchange_name = 'test_exchange'
        exchange_dict: Dict = {"name": exchange_name,
                               "exchange": True,
                               "api_url": 'https://url.to.api.com',
                               "requests": []}
        exchange = Exchange(exchange_dict)
        mappings = [Mapping('currency_pair_first', ['data', 'first'], ['str']),
                    Mapping('currency_pair_second', ['data', 'second'], ['str']),
                    Mapping('value', ['data', 'value'], ['str', 'int'])]
        method: str = 'ticker'
        exchange.response_mappings = {method: mappings}
        start_time = datetime.utcnow()
        time = datetime.utcnow()

        # noinspection PyTypeChecker
        response: Tuple[str, Dict[object, Optional[Dict]]] = (
        exchange_name, {None: [{'data': {'first': 'btc', 'second': 'eth', 'value': 5}},
                               {'data': {'first': 'eth', 'second': 'xrp', 'value': 6}},
                               {'data': {'first': 'btc', 'second': 'usd', 'value': 7}},
                               {'data': {'first': 'eth', 'second': 'usdt', 'value': 8}}]})

        result, keys = exchange.format_data(method, response, start_time, time)

        value_list = {(start_time, time, 'BTC', 'ETH', 5),
                      (start_time, time, 'ETH', 'XRP', 6),
                      (start_time, time, 'BTC', 'USD', 7),
                      (start_time, time, 'ETH', 'USDT', 8)}
        key_list = ['start_time', 'time', 'currency_pair_first', 'currency_pair_second', 'value']

        self.assertEqual(value_list, set(result))
        self.assertEqual(key_list, keys)

    def test_all_request_dict_list_dict(self):
        """Test for a request which contains all the available data."""
        exchange_name = 'test_exchange'
        exchange_dict: Dict = {"name": exchange_name,
                               "exchange": True,
                               "api_url": 'https://url.to.api.com',
                               "requests": []}
        exchange = Exchange(exchange_dict)
        mappings = [Mapping('currency_pair_first', ['data', 'first'], ['str']),
                    Mapping('currency_pair_second', ['data', 'second'], ['str']),
                    Mapping('value', ['data', 'value'], ['str', 'int'])]
        method: str = 'ticker'
        exchange.response_mappings = {method: mappings}
        start_time = datetime.utcnow()
        time = datetime.utcnow()

        # noinspection PyTypeChecker
        response: Tuple[str, Dict[object, Optional[Dict]]] = (
        exchange_name, {None: {'data': [{'first': 'btc', 'second': 'eth', 'value': 1},
                                        {'first': 'eth', 'second': 'xrp', 'value': 2},
                                        {'first': 'btc', 'second': 'usd', 'value': 3},
                                        {'first': 'eth', 'second': 'usdt', 'value': 4}]}})

        result, keys = exchange.format_data(method, response, start_time, time)

        value_list = {(start_time, time, 'BTC', 'ETH', 1),
                      (start_time, time, 'ETH', 'XRP', 2),
                      (start_time, time, 'BTC', 'USD', 3),
                      (start_time, time, 'ETH', 'USDT', 4)}
        key_list = ['start_time', 'time', 'currency_pair_first', 'currency_pair_second', 'value']

        self.assertEqual(value_list, set(result))
        self.assertEqual(key_list, keys)

    def test_cp_request_dict(self):
        """ Testing the formatting of individual responses for each currency pair.
            The data is contained in a dictionary."""
        exchange_name = 'test_exchange'
        exchange_dict: Dict = {"name": exchange_name,
                               "exchange": True,
                               "api_url": 'https://url.to.api.com',
                               "requests": []}
        exchange = Exchange(exchange_dict)
        mappings = [Mapping('value1', ['v1'], ['str', 'int']),
                    Mapping('value2', ['v2'], ['str']),
                    Mapping('value3', ['v3'], ['str'])]
        method: str = 'ticker'
        exchange.response_mappings = {method: mappings}
        start_time = datetime.utcnow()
        time = datetime.utcnow()

        cp1_mock: ExchangeCurrencyPair = Mock(spec=ExchangeCurrencyPair)
        cp1_mock.id = 1
        cp2_mock: ExchangeCurrencyPair = Mock(spec=ExchangeCurrencyPair)
        cp2_mock.id = 2
        cp3_mock: ExchangeCurrencyPair = Mock(spec=ExchangeCurrencyPair)
        cp3_mock.id = 3
        cp4_mock: ExchangeCurrencyPair = Mock(spec=ExchangeCurrencyPair)
        cp4_mock.id = 4

        # noinspection PyTypeChecker
        response: Tuple[str, Dict[object, Optional[Dict]]] = (
        exchange_name, {cp1_mock: {'v1': '10', 'v2': 'a', 'v3': 'b'},
                        cp2_mock: {'v1': '11', 'v2': 'c', 'v3': 'd'},
                        cp3_mock: {'v1': '12', 'v2': 'e', 'v3': 'f'},
                        cp4_mock: {'v1': '13', 'v2': 'g', 'v3': 'h'}})
        value_list = {(start_time, time, 10, 'A', 'B', 1),
                      (start_time, time, 11, 'C', 'D', 2),
                      (start_time, time, 12, 'E', 'F', 3),
                      (start_time, time, 13, 'G', 'H', 4)}
        key_list = ['start_time', 'time', 'value1', 'value2', 'value3', 'exchange_pair_id']

        result, keys = exchange.format_data(method, response, start_time, time)

        self.assertEqual(value_list, set(result))
        self.assertEqual(key_list, keys)

    def test_cp_request_dict_dict(self):
        """ Testing the formatting of individual responses for each currency pair.
            The data is guarded by a dict before accessing the actual data-dict."""
        exchange_name = 'test_exchange'
        exchange_dict: Dict = {"name": exchange_name,
                               "exchange": True,
                               "api_url": 'https://url.to.api.com',
                               "requests": []}
        exchange = Exchange(exchange_dict)
        mappings = [Mapping('value1', ['data', 'v1'], ['str', 'int']),
                    Mapping('value2', ['data', 'v2'], ['str']),
                    Mapping('value3', ['data', 'v3'], ['str'])]
        method: str = 'ticker'
        exchange.response_mappings = {method: mappings}
        start_time = datetime.utcnow()
        time = datetime.utcnow()

        cp1_mock: ExchangeCurrencyPair = Mock(spec=ExchangeCurrencyPair)
        cp1_mock.id = 1
        cp1_mock.first.name = 'btc'
        cp1_mock.first.name = 'eth'
        cp2_mock: ExchangeCurrencyPair = Mock(spec=ExchangeCurrencyPair)
        cp2_mock.id = 2
        cp1_mock.first.name = 'eth'
        cp1_mock.first.name = 'xrp'
        cp3_mock: ExchangeCurrencyPair = Mock(spec=ExchangeCurrencyPair)
        cp3_mock.id = 3
        cp1_mock.first.name = 'btc'
        cp1_mock.first.name = 'usd'
        cp4_mock: ExchangeCurrencyPair = Mock(spec=ExchangeCurrencyPair)
        cp4_mock.id = 4
        cp1_mock.first.name = 'btc'
        cp1_mock.first.name = 'usdt'

        # noinspection PyTypeChecker
        response: Tuple[str, Dict[object, Optional[Dict]]] = (
        exchange_name, {cp1_mock: {'data': {'v1': '10', 'v2': 'a', 'v3': 'b'}},
                        cp2_mock: {'data': {'v1': '11', 'v2': 'c', 'v3': 'd'}},
                        cp3_mock: {'data': {'v1': '12', 'v2': 'e', 'v3': 'f'}},
                        cp4_mock: {'data': {'v1': '13', 'v2': 'g', 'v3': 'h'}}})
        value_list = {(start_time, time, 10, 'A', 'B', 1),
                      (start_time, time, 11, 'C', 'D', 2),
                      (start_time, time, 12, 'E', 'F', 3),
                      (start_time, time, 13, 'G', 'H', 4)}
        key_list = ['start_time', 'time', 'value1', 'value2', 'value3', 'exchange_pair_id']

        result, keys = exchange.format_data(method, response, start_time, time)

        self.assertEqual(value_list, set(result))
        self.assertEqual(key_list, keys)

    def test_cp_request_dict_cp_guard_dict(self):
        """ Testing the formatting of individual responses for each currency pair.
            The desired data is guarded by the formatted currency pair string."""
        """ Testing the formatting of individual responses for each currency pair.
            The data is guarded by a dict before accessing the actual data-dict."""
        exchange_name = 'test_exchange'
        exchange_dict: Dict = {"name": exchange_name,
                               "exchange": True,
                               "api_url": 'https://url.to.api.com',
                               "requests": []}
        exchange = Exchange(exchange_dict)
        mappings = [Mapping('value1', ['data', 'v1'], ['str', 'int']),
                    Mapping('value2', ['data', 'v2'], ['str']),
                    Mapping('value3', ['data', 'v3'], ['str'])]
        method: str = 'ticker'
        exchange.response_mappings = {method: mappings}
        start_time = datetime.utcnow()
        time = datetime.utcnow()

        cp1_mock: ExchangeCurrencyPair = Mock(spec=ExchangeCurrencyPair)
        cp1_mock.id = 1
        cp2_mock: ExchangeCurrencyPair = Mock(spec=ExchangeCurrencyPair)
        cp2_mock.id = 2
        cp3_mock: ExchangeCurrencyPair = Mock(spec=ExchangeCurrencyPair)
        cp3_mock.id = 3
        cp4_mock: ExchangeCurrencyPair = Mock(spec=ExchangeCurrencyPair)
        cp4_mock.id = 4

        # noinspection PyTypeChecker
        response: Tuple[str, Dict[object, Optional[Dict]]] = (
        exchange_name, {cp1_mock: {'data': {'v1': '10', 'v2': 'a', 'v3': 'b'}},
                        cp2_mock: {'data': {'v1': '11', 'v2': 'c', 'v3': 'd'}},
                        cp3_mock: {'data': {'v1': '12', 'v2': 'e', 'v3': 'f'}},
                        cp4_mock: {'data': {'v1': '13', 'v2': 'g', 'v3': 'h'}}})
        value_list = {(start_time, time, 10, 'A', 'B', 1),
                      (start_time, time, 11, 'C', 'D', 2),
                      (start_time, time, 12, 'E', 'F', 3),
                      (start_time, time, 13, 'G', 'H', 4)}
        key_list = ['start_time', 'time', 'value1', 'value2', 'value3', 'exchange_pair_id']

        result, keys = exchange.format_data(method, response, start_time, time)

        self.assertEqual(value_list, set(result))
        self.assertEqual(key_list, keys)
