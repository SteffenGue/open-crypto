import asyncio
import unittest
from datetime import datetime
from typing import Dict, Any
from unittest.mock import MagicMock, Mock, patch, AsyncMock

from aiohttp import ClientConnectionError, ClientResponse

from model.database.tables import ExchangeCurrencyPair
from model.exchange.exchange import Exchange


class TestRequest(unittest.TestCase):
    def test_request_name_not_in_request_urls(self):
        """ Name of the request is not in the request_urls-dict."""
        exchange_dict: Dict = {"name": 'test_exchange', "exchange": True, "api_url": 'https://url.to.api.com', "requests": []}
        exchange = Exchange(exchange_dict)
        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(exchange.request("name that does not exist", []))
        self.assertIsNone(result)

    def test_request_name_empty_info(self):
        """Name of the request is in request-urls dict, but has no value behind it."""
        exchange_dict: Dict = {"name": 'test_exchange',
                               "exchange": True,
                               "api_url": 'https://url.to.api.com',
                               "requests": []}
        exchange = Exchange(exchange_dict)
        exchange.request_urls = {"ticker": {}}
        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(exchange.request("ticker", []))
        self.assertIsNone(result)

    def test_empty_currency_pairs(self):
        """Given list of currency-pairs is empty or none."""
        """Name of the request is in request-urls dict, but has no value behind it."""
        exchange_dict: Dict = {"name": 'test_exchange',
                               "exchange": True,
                               "api_url": 'https://url.to.api.com',
                               "requests": []}
        exchange = Exchange(exchange_dict)
        exchange.request_urls = {"ticker": {"params": {}, "pair_template": {}}}
        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(exchange.request("ticker", []))
        self.assertTrue(type(result[0]) == datetime)
        self.assertEqual('test_exchange', result[1])
        self.assertEqual({}, result[2])
        pass

    def test_failing_connection(self):
        """Test where the session is unable to retreive information."""
        http_mock = Mock(side_effect=ClientConnectionError) #mock will raise error when get() is called
        with patch('aiohttp.ClientSession.get', http_mock):
            exchange_dict: Dict = {"name": 'test_exchange',
                                   "exchange": True,
                                   "api_url": 'https://url.to.api.com',
                                   "requests": []}
            exchange = Exchange(exchange_dict)
            exchange.request_urls = {"ticker": {"url": {}, "params": {}, "pair_template": {}}}
            loop = asyncio.get_event_loop()
            ecp_mock = Mock(spec=ExchangeCurrencyPair)
            result = loop.run_until_complete(exchange.request('ticker', [ecp_mock]))

            http_mock.assert_called_with(url={}, params={})
            self.assertTrue(type(result[0]) == datetime)
            self.assertEqual('test_exchange', result[1])
            self.assertEqual({}, result[2])

    def test_unreadable_response(self):
        """Test where the response is not a json response."""
        http_mock = Mock(return_value="error 404 response not found")
        with patch('aiohttp.ClientSession.get', http_mock):
            exchange_dict: Dict = {"name": 'test_exchange',
                                   "exchange": True,
                                   "api_url": 'https://url.to.api.com',
                                   "requests": []}
            exchange = Exchange(exchange_dict)
            exchange.request_urls = {"ticker": {"url": {}, "params": {}, "pair_template": {}}}
            ecp_mock = Mock(spec=ExchangeCurrencyPair)

            loop = asyncio.get_event_loop()
            result = loop.run_until_complete(exchange.request('ticker', [ecp_mock]))

            http_mock.assert_called_with(url={}, params={})
            self.assertTrue(type(result[0]) == datetime)
            self.assertEqual('test_exchange', result[1])
            self.assertEqual({}, result[2])

    def test_regular_request_for_all(self):
        """Test for a request where all the information is retrieved with one request."""
        json_mock = Mock(spec=ClientResponse)
        json_mock.json.return_value = {"data": 'This is the response.'}
        http_mock = AsyncMock(return_value=json_mock)
        with patch('aiohttp.ClientSession.get', http_mock):
            exchange_dict: Dict = {"name": 'test_exchange',
                                   "exchange": True,
                                   "api_url": 'https://url.to.api.com',
                                   "requests": []}
            exchange = Exchange(exchange_dict)
            exchange.request_urls = {"ticker": {"url": {}, "params": {}, "pair_template": {}}}
            ecp_mock = Mock(spec=ExchangeCurrencyPair)

            loop = asyncio.get_event_loop()
            result = loop.run_until_complete(exchange.request('ticker', [ecp_mock]))
            json_mock.json.assert_called_once()
            http_mock.assert_called_with(url={}, params={})
            self.assertTrue(type(result[0]) == datetime)
            self.assertEqual('test_exchange', result[1])
            self.assertEqual({None: {'data': 'This is the response.'}}, result[2])

    def test_request_with_params(self):
        """Test for a regular request where parameters have to be sent."""
        json_mock = Mock(spec=ClientResponse)
        json_mock.json.return_value = {"data": 'This is the response.'}
        http_mock = AsyncMock(return_value=json_mock)
        with patch('aiohttp.ClientSession.get', http_mock):
            exchange_dict: Dict = {"name": 'test_exchange',
                                   "exchange": True,
                                   "api_url": 'https://url.to.api.com',
                                   "requests": []}
            exchange = Exchange(exchange_dict)
            exchange.request_urls = {"ticker": {"url": {},
                                                "params": {'steps': 1299, 'query_all': True},
                                                "pair_template": {}}}
            ecp_mock = Mock(spec=ExchangeCurrencyPair)

            loop = asyncio.get_event_loop()
            result = loop.run_until_complete(exchange.request('ticker', [ecp_mock]))
            json_mock.json.assert_called_once()
            http_mock.assert_called_with(url={}, params={'steps': 1299, 'query_all': True})
            self.assertTrue(type(result[0]) == datetime)
            self.assertEqual('test_exchange', result[1])
            self.assertEqual({None: {'data': 'This is the response.'}}, result[2])

    def test_request_with_pair_formatting_in_url(self):
        """Test for a request where the pair info has to be sent as a string in a certain format."""
        json_mock = Mock(spec=ClientResponse)
        json_mock.json.return_value = {"data": 'This is the response.'}
        http_mock = AsyncMock(return_value=json_mock)
        with patch('aiohttp.ClientSession.get', http_mock):
            exchange_dict: Dict = {"name": 'test_exchange',
                                   "exchange": True,
                                   "api_url": 'https://url.to.api.com',
                                   "requests": []}
            exchange = Exchange(exchange_dict)
            exchange.request_urls = {"ticker": {"url": 'ticker/{currency_pair}',
                                                "params": {},
                                                "pair_template": {'template': '{first}_{second}', 'lower_case': True}}}
            ecp_mock = Mock(spec=ExchangeCurrencyPair)
            ecp_mock.first.name = 'BTC'
            ecp_mock.second.name = 'ETH'

            loop = asyncio.get_event_loop()
            result = loop.run_until_complete(exchange.request('ticker', [ecp_mock]))
            json_mock.json.assert_called_once()
            http_mock.assert_called_with(url='ticker/btc_eth', params={})
            self.assertTrue(type(result[0]) == datetime)
            self.assertEqual('test_exchange', result[1])
            self.assertEqual({ecp_mock: {'data': 'This is the response.'}}, result[2])

    def test_request_with_pair_formatting_as_param(self):
        """Test for a request where the pair info has to be sent as a formatted string as parameter."""
        json_mock = Mock(spec=ClientResponse)
        json_mock.json.return_value = {"data": 'This is the response.'}
        http_mock = AsyncMock(return_value=json_mock)
        with patch('aiohttp.ClientSession.get', http_mock):
            exchange_dict: Dict = {"name": 'test_exchange',
                                   "exchange": True,
                                   "api_url": 'https://url.to.api.com',
                                   "requests": []}
            exchange = Exchange(exchange_dict)
            exchange.request_urls = {"ticker": {"url": '',
                                                "params": {},
                                                "pair_template": {'template': '{first}_{second}', 'lower_case': True, 'alias': 'pair'}}}
            ecp_mock = Mock(spec=ExchangeCurrencyPair)
            ecp_mock.first.name = 'BTC'
            ecp_mock.second.name = 'ETH'

            loop = asyncio.get_event_loop()
            result = loop.run_until_complete(exchange.request('ticker', [ecp_mock]))
            json_mock.json.assert_called_once()
            http_mock.assert_called_with(url='', params={'pair': 'btc_eth'})
            self.assertTrue(type(result[0]) == datetime)
            self.assertEqual('test_exchange', result[1])
            self.assertEqual({ecp_mock: {'data': 'This is the response.'}}, result[2])
