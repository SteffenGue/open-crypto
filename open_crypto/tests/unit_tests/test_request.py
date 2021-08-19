# import asyncio
# from datetime import datetime
# from typing import Dict
# from unittest.mock import Mock, patch, AsyncMock
#
# from aiohttp import ClientConnectionError, ClientResponse
#
# from model.database.tables import ExchangeCurrencyPair, Ticker
# from model.exchange.exchange import Exchange
#
#
# ToDO: FIXEN
# class TestRequest:
#     def test_request_name_not_in_request_urls(self):
#         """ Name of the request is not in the request_urls-dict."""
#         exchange_dict: Dict = {"name": 'test_exchange', "exchange": True, "api_url": 'https://url.to.api.com',
#                                "requests": []}
#         exchange = Exchange(exchange_dict, None, None)
#         loop = asyncio.get_event_loop()
#         # result = loop.run_until_complete(exchange.request("name that does not exist", []))
#         result = loop.run_until_complete(exchange.request(Ticker, []))
#
#         assert result is None
#
#     def test_request_name_empty_info(self):
#         """Name of the request is in request-urls dict, but has no value behind it."""
#         exchange_dict: Dict = {"name": 'test_exchange',
#                                "exchange": True,
#                                "api_url": 'https://url.to.api.com',
#                                "requests": []}
#         exchange = Exchange(exchange_dict, None, None)
#         exchange.request_urls = {"tickers": {}}
#         loop = asyncio.get_event_loop()
#         result = loop.run_until_complete(exchange.request("tickers", []))
#         assert result is None
#
#     def test_empty_currency_pairs(self):
#         """Given list of currency-pairs is empty or none."""
#         """Name of the request is in request-urls dict, but has no value behind it."""
#         exchange_dict: Dict = {"name": 'test_exchange',
#                                "exchange": True,
#                                "api_url": 'https://url.to.api.com',
#                                "requests": []}
#         exchange = Exchange(exchange_dict, None, None)
#         exchange.request_urls = {"tickers": {'url': "", "params": {}, "pair_template": {}}}
#         loop = asyncio.get_event_loop()
#         result = loop.run_until_complete(exchange.request("tickers", []))
#         assert type(result[0]) == datetime
#         assert 'test_exchange' == result[1]
#         assert {} == result[2]
#
#     def test_failing_connection(self):
#         """Test where the session is unable to retreive information."""
#         http_mock = Mock(side_effect=ClientConnectionError)  # mock will raise error when get() is called
#         with patch('aiohttp.ClientSession.get', http_mock):
#             exchange_dict: Dict = {"name": 'test_exchange',
#                                    "exchange": True,
#                                    "api_url": 'https://url.to.api.com',
#                                    "requests": []}
#             exchange = Exchange(exchange_dict, None, None)
#             exchange.request_urls = {"tickers": {"url": {}, "params": {}, "pair_template": {}}}
#             loop = asyncio.get_event_loop()
#             ecp_mock = Mock(spec=ExchangeCurrencyPair)
#             result = loop.run_until_complete(exchange.request('tickers', [ecp_mock]))
#
#             http_mock.assert_called_with(url={}, params={})
#             assert type(result[0]) == datetime
#             assert 'test_exchange' == result[1]
#             assert {} == result[2]
#
#     def test_unreadable_response(self):
#         """Test where the response is not a json response."""
#         http_mock = Mock(return_value="error 404 response not found")
#         with patch('aiohttp.ClientSession.get', http_mock):
#             exchange_dict: Dict = {"name": 'test_exchange',
#                                    "exchange": True,
#                                    "api_url": 'https://url.to.api.com',
#                                    "requests": []}
#             exchange = Exchange(exchange_dict, None, None)
#             exchange.request_urls = {"tickers": {"url": {}, "params": {}, "pair_template": {}}}
#             ecp_mock = Mock(spec=ExchangeCurrencyPair)
#
#             loop = asyncio.get_event_loop()
#             result = loop.run_until_complete(exchange.request('tickers', [ecp_mock]))
#
#             http_mock.assert_called_with(url={}, params={})
#             assert type(result[0]) == datetime
#             assert 'test_exchange' == result[1]
#             assert {} == result[2]
#
#     def test_regular_request_for_all(self):
#         """Test for a request where all the information is retrieved with one request."""
#         json_mock = Mock(spec=ClientResponse)
#         json_mock.json.return_value = {"data": 'This is the response.'}
#         http_mock = AsyncMock(return_value=json_mock)
#         with patch('aiohttp.ClientSession.get', http_mock):
#             exchange_dict: Dict = {"name": 'test_exchange',
#                                    "exchange": True,
#                                    "api_url": 'https://url.to.api.com',
#                                    "requests": []}
#             exchange = Exchange(exchange_dict, None, None)
#             exchange.request_urls = {"tickers": {"url": {}, "params": {}, "pair_template": {}}}
#             ecp_mock = Mock(spec=ExchangeCurrencyPair)
#
#             loop = asyncio.get_event_loop()
#             result = loop.run_until_complete(exchange.request('tickers', [ecp_mock]))
#             json_mock.json.assert_called_once()
#             http_mock.assert_called_with(url={}, params={})
#             assert type(result[0]) == datetime
#             assert 'test_exchange' == result[1]
#             assert {None: {'data': 'This is the response.'}} == result[2]
#
#     def test_request_with_params(self):
#         """Test for a regular request where parameters have to be sent."""
#         json_mock = Mock(spec=ClientResponse)
#         json_mock.json.return_value = {"data": 'This is the response.'}
#         http_mock = AsyncMock(return_value=json_mock)
#         with patch('aiohttp.ClientSession.get', http_mock):
#             exchange_dict: Dict = {"name": 'test_exchange',
#                                    "exchange": True,
#                                    "api_url": 'https://url.to.api.com',
#                                    "requests": []}
#             exchange = Exchange(exchange_dict)
#             exchange.request_urls = {"tickers": {"url": {},
#                                                  "params": {'steps': 1299, 'query_all': True},
#                                                  "pair_template": {}}}
#             ecp_mock = Mock(spec=ExchangeCurrencyPair)
#
#             loop = asyncio.get_event_loop()
#             result = loop.run_until_complete(exchange.request('tickers', [ecp_mock]))
#             json_mock.json.assert_called_once()
#             http_mock.assert_called_with(url={}, params={'steps': 1299, 'query_all': True})
#             assert type(result[0]) == datetime
#             assert 'test_exchange' == result[1]
#             assert {None: {'data': 'This is the response.'}} == result[2]
#
#     def test_request_with_pair_formatting_in_url(self):
#         """Test for a request where the pair info has to be sent as a string in a certain format."""
#         json_mock = Mock(spec=ClientResponse)
#         json_mock.json.return_value = {"data": 'This is the response.'}
#         http_mock = AsyncMock(return_value=json_mock)
#         with patch('aiohttp.ClientSession.get', http_mock):
#             exchange_dict: Dict = {"name": 'test_exchange',
#                                    "exchange": True,
#                                    "api_url": 'https://url.to.api.com',
#                                    "requests": []}
#             exchange = Exchange(exchange_dict, None, None)
#             exchange.request_urls = {"tickers": {"url": 'tickers/{currency_pair}',
#                                                  "params": {},
#                                                  "pair_template": {'template': '{first}_{second}', 'lower_case': True}}}
#             ecp_mock = Mock(spec=ExchangeCurrencyPair)
#             ecp_mock.first.name = 'BTC'
#             ecp_mock.second.name = 'ETH'
#
#             loop = asyncio.get_event_loop()
#             result = loop.run_until_complete(exchange.request('tickers', [ecp_mock]))
#             json_mock.json.assert_called_once()
#             http_mock.assert_called_with(url='tickers/btc_eth', params={})
#             assert type(result[0]) == datetime
#             assert 'test_exchange' == result[1]
#             assert {None: {'data': 'This is the response.'}} == result[2]
#
#     def test_request_with_pair_formatting_as_param(self):
#         """Test for a request where the pair info has to be sent as a formatted string as parameter."""
#         json_mock = Mock(spec=ClientResponse)
#         json_mock.json.return_value = {"data": 'This is the response.'}
#         http_mock = AsyncMock(return_value=json_mock)
#         with patch('aiohttp.ClientSession.get', http_mock):
#             exchange_dict: Dict = {"name": 'test_exchange',
#                                    "exchange": True,
#                                    "api_url": 'https://url.to.api.com',
#                                    "requests": []}
#             exchange = Exchange(exchange_dict, None, None)
#             exchange.request_urls = {"tickers": {"url": '',
#                                                  "params": {},
#                                                  "pair_template": {'template': '{first}_{second}', 'lower_case': True,
#                                                                    'alias': 'pair'}}}
#             ecp_mock = Mock(spec=ExchangeCurrencyPair)
#             ecp_mock.first.name = 'BTC'
#             ecp_mock.second.name = 'ETH'
#
#             loop = asyncio.get_event_loop()
#             result = loop.run_until_complete(exchange.request('tickers', [ecp_mock]))
#             json_mock.json.assert_called_once()
#             http_mock.assert_called_with(url='', params={'pair': 'btc_eth'})
#             assert type(result[0]) == datetime
#             assert 'test_exchange' == result[1]
#             assert {None: {'data': 'This is the response.'}} == result[2]