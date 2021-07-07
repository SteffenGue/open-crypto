#!/usr/bin/env python
# -*- coding: utf-8 -*-
# type: ignore[no-untyped-def]
"""
TODO: Fill out module docstring.
"""

from typing import Dict, Tuple, Optional
from unittest.mock import Mock

import pytest

from model.database.tables import ExchangeCurrencyPair
from model.exchange.exchange import Exchange
from model.exchange.mapping import Mapping
from model.utilities.exceptions import MappingNotFoundException, DifferentExchangeContentException, \
    NoCurrencyPairProvidedException
from model.utilities.time_helper import TimeHelper


@pytest.fixture(name="exchange")
def create_exchange() -> Exchange:
    """
    TODO: Fill out
    """
    exchange_name = "test_exchange"
    exchange_dict = {
        "name": exchange_name,
        "exchange": True,
        "api_url": "https://url.to.api.com",
        "requests": []
    }
    return Exchange(exchange_dict, None, None)


class TestFormatData:
    """
    TODO: Fill out
    """

    def test_response_is_none(self, exchange):
        """Testing the handling of a response with no data in it."""
        # setup
        mappings = [Mapping("first_currency", ["data"], ["int"])]
        method = "ticker"
        exchange.response_mappings = {method: mappings}
        start_time = TimeHelper.now()
        time = TimeHelper.now()

        # test for ticker-data for all available currency-pairs
        response: Tuple[str, Dict[object, Optional[Dict]]] = (exchange.name, {None: None})
        result = exchange.format_data(method, response, start_time, time)

        with pytest.raises(StopIteration):
            next(result)

        # test for multiple currency-pairs
        first_ecp_mock = Mock(spec=ExchangeCurrencyPair)
        second_ecp_mock = Mock(spec=ExchangeCurrencyPair)
        third_ecp_mock = Mock(spec=ExchangeCurrencyPair)
        response = (exchange.name, {first_ecp_mock: None, second_ecp_mock: None, third_ecp_mock: None})
        result = exchange.format_data(method, response, start_time, time)

        with pytest.raises(StopIteration):
            next(result)

    def test_empty_response(self, exchange):
        """Testing the handling of an empty response."""
        # todo: test for pairs and for all together
        # setup
        mappings = [Mapping("first_currency", ["data"], ["int"])]
        method = "ticker"
        exchange.response_mappings = {method: mappings}
        start_time = TimeHelper.now()
        time = TimeHelper.now()

        # test for ticker-data for all available currency-pairs
        response: Tuple[str, Dict[object, Optional[Dict]]] = (exchange.name, {None: {}})
        result = exchange.format_data(method, response, start_time, time)

        with pytest.raises(StopIteration):
            next(result)

        # test for multiple currency-pairs
        first_ecp_mock = Mock(spec=ExchangeCurrencyPair)
        second_ecp_mock = Mock(spec=ExchangeCurrencyPair)
        third_ecp_mock = Mock(spec=ExchangeCurrencyPair)
        response = (exchange.name, {first_ecp_mock: {}, second_ecp_mock: {}, third_ecp_mock: {}})

        result = exchange.format_data(method, response, start_time, time)

        with pytest.raises(StopIteration):
            next(result)

    def test_no_mapping_available(self, exchange):
        """Testing the case if the data is valid but there is no mapping available."""
        # Method is there but no mappings behind
        mappings = []
        method: str = "ticker"
        exchange.response_mappings = {method: mappings}
        start_time = TimeHelper.now()
        time = TimeHelper.now()

        response: Tuple[str, Dict[object, Optional[Dict]]] = (exchange.name, {None: {"response": "Hello"}})
        with pytest.raises(MappingNotFoundException):
            next(exchange.format_data(method, response, start_time, time))

        # No method available
        exchange.response_mappings = {}
        with pytest.raises(MappingNotFoundException):
            next(exchange.format_data(method, response, start_time, time))

    def test_response_from_diff_exchange(self, exchange):
        """Testing the case where the given response-dict is from a different exchange.
           This is detected by the given name."""
        mappings = []
        method: str = "ticker"
        exchange.response_mappings = {method: mappings}
        start_time = TimeHelper.now()
        time = TimeHelper.now()

        response: Tuple[str, Dict[object, Optional[Dict]]] = ("other exchange", {None: {"response": "Hello"}})
        with pytest.raises(DifferentExchangeContentException):
            next(exchange.format_data(method, response, start_time, time))

    def test_all_request_but_no_cp_first_or_second(self, exchange):
        """ Testing special case where the response contains all the available data but
            there is no currency_pair_first or currency_pair_second as name for a mapping."""

        method: str = "ticker"
        # currency_pair_first missing
        mappings = [Mapping("currency_pair_second", ["second"], ["str"]),
                    Mapping("value", ["value"], ["str", "int"])]
        exchange.response_mappings = {method: mappings}
        start_time = TimeHelper.now()
        time = TimeHelper.now()

        # noinspection PyTypeChecker
        response: Tuple[str, Dict[object, Optional[Dict]]] = (
            exchange.name, {None: [{"first": "btc", "second": "eth", "value": 1},
                                   {"first": "eth", "second": "xrp", "value": 2},
                                   {"first": "btc", "second": "usd", "value": 3},
                                   {"first": "eth", "second": "usdt", "value": 4}]})
        with pytest.raises(NoCurrencyPairProvidedException):
            next(exchange.format_data(method, response, start_time, time))

        # currency_pair_second missing
        mappings = [Mapping("currency_pair_first", ["first"], ["str"]),
                    Mapping("value", ["value"], ["str", "int"])]
        exchange.response_mappings = {method: mappings}
        with pytest.raises(NoCurrencyPairProvidedException):
            next(exchange.format_data(method, response, start_time, time))

        # both missing
        mappings = [Mapping("value", ["value"], ["str", "int"])]
        exchange.response_mappings = {method: mappings}
        with pytest.raises(NoCurrencyPairProvidedException):
            next(exchange.format_data(method, response, start_time, time))

    def test_all_request_guarding_cp(self):
        """ Testing format_data for a response that coontains all the available data.
            The data for each currency pair is guarded by the formatted string from each pair."""
        # todo: currently there is no way to get to this information lol

    def test_all_request_list_dict(self, exchange):
        """ Test for a request which contains all the available data.
            The mocked response is a list of dictionaries which contain the data that
            is to be formatted."""

        mappings = [Mapping("currency_pair_first", ["first"], ["str"]),
                    Mapping("currency_pair_second", ["second"], ["str"]),
                    Mapping("value", ["value"], ["str", "int"])]
        method: str = "ticker"
        exchange.response_mappings = {method: mappings}
        start_time = TimeHelper.now()
        time = TimeHelper.now()

        # noinspection PyTypeChecker
        response: Tuple[str, Dict[object, Optional[Dict]]] = (
            exchange.name, {None: [{"first": "btc", "second": "eth", "value": 1},
                                   {"first": "eth", "second": "xrp", "value": 2},
                                   {"first": "btc", "second": "usd", "value": 3},
                                   {"first": "eth", "second": "usdt", "value": 4}]})

        # result, keys = exchange.format_data(method, response, start_time, time)
        result = exchange.format_data(method, response, start_time, time)

        value_list = [(start_time, time, "btc", "eth", 1),
                      (start_time, time, "eth", "xrp", 2),
                      (start_time, time, "btc", "usd", 3),
                      (start_time, time, "eth", "usdt", 4)]
        key_list = ["start_time", "time", "currency_pair_first", "currency_pair_second", "value"]

        for got in result:
            assert value_list == got[0]
            assert key_list == got[1]

    def test_all_request_list_dict_dict(self, exchange):
        """Test for a request which contains all the available data."""
        mappings = [Mapping("currency_pair_first", ["data", "first"], ["str"]),
                    Mapping("currency_pair_second", ["data", "second"], ["str"]),
                    Mapping("value", ["data", "value"], ["str", "int"])]
        method: str = "ticker"
        exchange.response_mappings = {method: mappings}
        start_time = TimeHelper.now()
        time = TimeHelper.now()

        # noinspection PyTypeChecker
        response: Tuple[str, Dict[object, Optional[Dict]]] = (
            exchange.name, {None: [{"data": {"first": "btc", "second": "eth", "value": 5}},
                                   {"data": {"first": "eth", "second": "xrp", "value": 6}},
                                   {"data": {"first": "btc", "second": "usd", "value": 7}},
                                   {"data": {"first": "eth", "second": "usdt", "value": 8}}]})

        result = exchange.format_data(method, response, start_time, time)

        value_list = [(start_time, time, "btc", "eth", 5),
                      (start_time, time, "eth", "xrp", 6),
                      (start_time, time, "btc", "usd", 7),
                      (start_time, time, "eth", "usdt", 8)]
        key_list = ["start_time", "time", "currency_pair_first", "currency_pair_second", "value"]

        for got in result:
            assert value_list == got[0]
            assert key_list == got[1]

    def test_all_request_dict_list_dict(self, exchange):
        """Test for a request which contains all the available data."""

        mappings = [Mapping("currency_pair_first", ["data", "first"], ["str"]),
                    Mapping("currency_pair_second", ["data", "second"], ["str"]),
                    Mapping("value", ["data", "value"], ["str", "int"])]
        method: str = "ticker"
        exchange.response_mappings = {method: mappings}
        start_time = TimeHelper.now()
        time = TimeHelper.now()

        # noinspection PyTypeChecker
        response: Tuple[str, Dict[object, Optional[Dict]]] = (
            exchange.name, {None: {"data": [{"first": "btc", "second": "eth", "value": 1},
                                            {"first": "eth", "second": "xrp", "value": 2},
                                            {"first": "btc", "second": "usd", "value": 3},
                                            {"first": "eth", "second": "usdt", "value": 4}]}})

        result = exchange.format_data(method, response, start_time, time)

        value_list = [(start_time, time, "btc", "eth", 1),
                      (start_time, time, "eth", "xrp", 2),
                      (start_time, time, "btc", "usd", 3),
                      (start_time, time, "eth", "usdt", 4)]
        key_list = ["start_time", "time", "currency_pair_first", "currency_pair_second", "value"]

        for got in result:
            assert value_list == got[0]
            assert key_list == got[1]

    def test_cp_request_dict(self, exchange):
        """ Testing the formatting of individual responses for each currency pair.
            The data is contained in a dictionary."""
        exchange.request_urls = {'ticker': {'pair_template': {'template': '{first}_{second}', 'lower_case': False}}}
        mappings = [Mapping('value1', ['v1'], ['str', 'int']),
                    Mapping('value2', ['v2'], ['str']),
                    Mapping('value3', ['v3'], ['str'])]
        method: str = 'ticker'
        exchange.response_mappings = {method: mappings}
        start_time = TimeHelper.now()
        time = TimeHelper.now()

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
            exchange.name, {cp1_mock: {'v1': '10', 'v2': 'a', 'v3': 'b'},
                            cp2_mock: {'v1': '11', 'v2': 'c', 'v3': 'd'},
                            cp3_mock: {'v1': '12', 'v2': 'e', 'v3': 'f'},
                            cp4_mock: {'v1': '13', 'v2': 'g', 'v3': 'h'}})
        value_list = [(start_time, time, 10, 'a', 'b', 1),
                      (start_time, time, 11, 'c', 'd', 2),
                      (start_time, time, 12, 'e', 'f', 3),
                      (start_time, time, 13, 'g', 'h', 4)]
        key_list = ['start_time', 'time', 'value1', 'value2', 'value3', 'exchange_pair_id']

        result = exchange.format_data(method, response, start_time, time)

        for i, got in enumerate(result):
            assert value_list[i] == got[0][0]
            assert key_list == got[1]

    def test_cp_request_dict_dict(self, exchange):
        """ Testing the formatting of individual responses for each currency pair.
            The data is guarded by a dict before accessing the actual data-dict."""
        exchange.request_urls = {'ticker': {'pair_template': {'template': '{first}_{second}', 'lower_case': False}}}
        mappings = [Mapping('value1', ['data', 'v1'], ['str', 'int']),
                    Mapping('value2', ['data', 'v2'], ['str']),
                    Mapping('value3', ['data', 'v3'], ['str'])]
        method: str = 'ticker'
        exchange.response_mappings = {method: mappings}
        start_time = TimeHelper.now()
        time = TimeHelper.now()

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
            exchange.name, {cp1_mock: {'data': {'v1': '10', 'v2': 'a', 'v3': 'b'}},
                            cp2_mock: {'data': {'v1': '11', 'v2': 'c', 'v3': 'd'}},
                            cp3_mock: {'data': {'v1': '12', 'v2': 'e', 'v3': 'f'}},
                            cp4_mock: {'data': {'v1': '13', 'v2': 'g', 'v3': 'h'}}})
        value_list = [(start_time, time, 10, 'a', 'b', 1),
                      (start_time, time, 11, 'c', 'd', 2),
                      (start_time, time, 12, 'e', 'f', 3),
                      (start_time, time, 13, 'g', 'h', 4)]
        key_list = ['start_time', 'time', 'value1', 'value2', 'value3', 'exchange_pair_id']

        result = exchange.format_data(method, response, start_time, time)

        for i, got in enumerate(result):
            assert value_list[i] == got[0][0]
            assert key_list == got[1]

    def test_cp_request_dict_cp_guard_dict(self, exchange):
        """ Testing the formatting of individual responses for each currency pair.
            The desired data is guarded by the formatted currency pair string."""
        exchange.request_urls = {'ticker': {'pair_template': {'template': '{first}_{second}', 'lower_case': False}}}
        mappings = [Mapping('value1', ['data', 'v1'], ['str', 'int']),
                    Mapping('value2', ['data', 'v2'], ['str']),
                    Mapping('value3', ['data', 'v3'], ['str'])]
        method: str = 'ticker'
        exchange.response_mappings = {method: mappings}
        start_time = TimeHelper.now()
        time = TimeHelper.now()

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
            exchange.name, {cp1_mock: {'data': {'v1': '10', 'v2': 'a', 'v3': 'b'}},
                            cp2_mock: {'data': {'v1': '11', 'v2': 'c', 'v3': 'd'}},
                            cp3_mock: {'data': {'v1': '12', 'v2': 'e', 'v3': 'f'}},
                            cp4_mock: {'data': {'v1': '13', 'v2': 'g', 'v3': 'h'}}})
        value_list = [(start_time, time, 10, 'a', 'b', 1),
                      (start_time, time, 11, 'c', 'd', 2),
                      (start_time, time, 12, 'e', 'f', 3),
                      (start_time, time, 13, 'g', 'h', 4)]
        key_list = ['start_time', 'time', 'value1', 'value2', 'value3', 'exchange_pair_id']

        result = exchange.format_data(method, response, start_time, time)

        for i, got in enumerate(result):
            assert value_list[i] == got[0][0]
            assert key_list == got[1]
