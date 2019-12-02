import itertools
from datetime import datetime
from typing import Any, Tuple, List, Iterator

import aiohttp
import requests
from aiohttp import ClientConnectionError, ClientConnectorError

from Mapping import Mapping
from tables import Ticker


class RateLimit(object):
    limit: int
    unit: int

    def __init__(self, limit: int, unit: int):
        self.limit = limit
        self.unit = unit


class Exchange:
    name: str
    terms_url: str
    scrape_permission: bool
    api_url: str
    rate_limit: RateLimit
    request_urls: {}
    response_mappings: {}
    yaml_file: dict

    def __init__(self, yaml_file: dict):
        self.yaml_file = yaml_file
        self.name = yaml_file['name']
        if yaml_file.get('terms'):
            if yaml_file['terms'].get('terms_url'):
                self.terms_url = yaml_file['terms']['terms_url']
            if yaml_file['terms'].get('permission'):
                self.scrape_permission = yaml_file['terms']['permission']

        self.api_url = yaml_file['api_url']
        if yaml_file.get('rate_limit'):
            self.rate_limit = yaml_file['rate_limit']
        self.request_urls = self.extract_request_urls(yaml_file['requests'])
        self.response_mappings = self.extract_mappings(
            yaml_file['requests'])  # Dict in dem für jede Request eine Liste von Mappings ist

    # async def request(self, request_name: str) -> (str, datetime, dict):
    #     async with aiohttp.ClientSession() as session:
    #         response = await session.get(self.api_url + self.request_urls[request_name])
    #         result = (self.name, datetime.now(), json.loads(await response.read()))
    #         return result

    async def request(self, request_name: str) -> [str, datetime, dict]:
        # Only when request url exists
        if self.request_urls.get(request_name):
            async with aiohttp.ClientSession() as session:
                request_url_and_params = self.request_urls[request_name]
                try:
                    response = await session.get(request_url_and_params[0], params=request_url_and_params[1])
                    response_json = await response.json(content_type=None)
                    print('{} bekommen.'.format(request_url_and_params[0]))
                    # with open('responses/{}'.format(self.name + '.json'), 'w', encoding='utf-8') as f:
                    #     json.dump(response_json, f, ensure_ascii=False, indent=4)
                    return self.name, datetime.now(), response_json
                except ClientConnectionError:
                    print('{} hat einen ConnectionError erzeugt.'.format(self.name))
                except Exception as ex:
                    template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                    message = template.format(type(ex).__name__, ex.args)
                    print(message)
                    print('Die Response von {} konnte nicht gelesen werden.'.format(self.name))
                except ClientConnectorError:
                    print('{} hat nicht geantwortet.'.format(self.name))
                return None
        else:
            return None

    def extract_request_urls(self, requests: dict) -> dict:
        urls = dict()

        for request in requests:
            url = self.api_url
            request_dict = requests[request]['request']
            #
            # TODO FIND A WAY TO PASS PAIR TEMPLATE
            # if 'pair_template' in request_dict.keys() and request_dict['pair_template']:

            #     template_dict = request_dict['pair_template']
            #     pair_str = '{}={}'.format(template_dict['alias'], template_dict['template'])
            #     url += pair_str
            #
            if 'template' in request_dict.keys() and request_dict['template']:
                url += '{}'.format(request_dict['template'])
            #

            params = dict()
            if 'params' in request_dict.keys() and request_dict['params']:
                for param in request_dict['params']:
                    # url += '&{}={}'.format(param, request_dict['params'][param]['default'])
                    params[param] = str(request_dict['params'][param]['default'])

            urls[request] = (url, params)

        return urls

    def extract_mappings(self, requests: dict) -> dict:
        response_mappings = dict()
        for request in requests:
            request_mapping: dict = self.yaml_file['requests'][request]

            if 'mapping' in request_mapping.keys():
                mapping = request_mapping['mapping']
                mapping_list = list()

                for entry in mapping:
                    mapping_list.append(Mapping(entry['key'], entry['path'], entry['type']))

                response_mappings[request] = mapping_list

        return response_mappings
    #[name, zeit, response.json]
    def get_ticker(self, response: list) -> Iterator:
        result = {'currency_pair_first': [],
                  'currency_pair_second': [],
                  'ticker_last_price': [],
                  'ticker_last_trade': [],
                  'ticker_best_ask': [],
                  'ticker_best_bid': [],
                  'ticker_daily_volume': []}

        mappings = self.response_mappings['ticker']
        for mapping in mappings:
            result[mapping.key] = mapping.extract_value(response[2])
            print(result)
        result = list(itertools.zip_longest(itertools.repeat(self.name,  len(result['currency_pair_first'])),
                                            itertools.repeat(response[1], len(result['currency_pair_first'])),
                                            result['currency_pair_first'],
                                            result['currency_pair_second'],
                                            result['ticker_last_price'],
                                            result['ticker_last_trade'],
                                            result['ticker_best_ask'],
                                            result['ticker_best_bid'],
                                            result['ticker_daily_volume']))
        return result
