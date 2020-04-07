import itertools
from datetime import datetime
from typing import Iterator, Dict, List, Tuple
import aiohttp
from aiohttp import ClientConnectionError, ClientConnectorError
from Mapping import Mapping
from utilities import REQUEST_PARAMS


class Exchange:
    """
    Attributes:
        Embodies the characteristics of a crypto-currency-exchange.
        Each exchange is an Exchange-object.
        Each 'job' is in the end a method called on every exchange.

        The Attributes and mappings are all extracted from the
        .yaml-files whose location is described in the config.ini file.

        name: str
            Name of this exchange.
        terms_url: str
            Url to Terms&Conditions of this exchange.
        scrape_permission: bool
            Permission if scraping data from this exchange is permitted.
        api_url: str
            Url for the public_api of this exchange.
        request_urls: dict[request_name: List[url, params]
            Dictionary which contains for each request(key)
            the given url and necessary parameters.
            (See .yaml and def extract_request_urls() for more info)
        response_mappings: dict
            Dictionary which contains for each request(key)
            the necessary mapping-objects for extracting the value.
            (See .yaml and def extract_mappings() for more info)
    """
    name: str
    terms_url: str
    scrape_permission: bool
    api_url: str
    request_urls: dict
    response_mappings: dict
    exception_counter: int
    consecutive_exception: bool
    active_flag: bool

    def __init__(self, yaml_file: Dict, database_handler_request_params):
        """
        Creates a new Exchange-object.

        Checks the content of the .yaml if it contains certain keys.
        If searched keys exist, the constructor sets the values for the described attributes.
        The constructor also calls extract_request_urls() and extract_mappings()
        to create request_urls and the necessary Mapping-Objects.

        :param yaml_file: Dict
            Content of a .yaml file as a dict-object.
            Constructor does not check if content is viable.

        :param database_handler_request_params: Function
            DatabaseHandler-Function from the main() database_handler instance.
            This is necessary to perform function calls from the request parameters which include
            database queries. Database connections should only take place from DatabaseHandler instances.
        """
        self.request_params = database_handler_request_params

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
        self.exception_counter = 0
        self.active_flag = True
        self.consecutive_exception = False

    async def request(self, request_name: str, start_time: datetime) -> Tuple[str, datetime, datetime, Dict]:
        """
        Sends a request which is identified by the given name and returns
        the response with the name of this exchange and the time,
        when the response arrived.

        The Method is asynchronous so that after the request is send, the program does not wait
        until the response arrives. For asynchrony we use the library asyncio.
        For sending and dealing with requests/responses the library aiohttp is used.

        The methods gets the requests matching url out of request_urls.
        If it does not exist None will be returned. Otherwise it sends and awaits the response(.json)
        and tries afterwards to parse the json to a dictionary.
        The parsed json is then returned with the name of this exchange, so
        the response is assignable to this exchange and the time when the response arrived.

        Exceptions will be caught and a suitable message is printed.
        None will be returned in this case.

        TODO: Gutes differneziertes Exception handling
        TODO: Logging von Exceptions / option in config
        TODO: Saving responses / option in config

        :param request_name: str
            Name of the request. i.e. 'ticker' for ticker-request
        :param start_time : datetime
            The given datetime for the request for each request loop.

        :return: (str, datetime, datetime, .json)
            Tuple of the following structure:
                (exchange_name, start time, response time, response)
                - time of arrival is a datetime-object in utc
        :exceptions ClientConnectionError: the connection to the exchange timed out or the exchange did not answered
                    Exception: the given response of an exchange could not be evaluated
        """
        if self.request_urls.get(request_name): # Only when request url exists
            async with aiohttp.ClientSession() as session:
                request_url_and_params = self.request_urls[request_name]
                try:
                    response = await session.get(request_url_and_params[0], params=request_url_and_params[1])
                    response_json = await response.json(content_type=None)
                    print('{} bekommen.'.format(request_url_and_params[0]))
                    # with open('responses/{}'.format(self.name + '.json'), 'w', encoding='utf-8') as f:
                    #     json.dump(response_json, f, ensure_ascii=False, indent=4)
                    self.consecutive_exception = False
                    return self.name, start_time, datetime.utcnow(), response_json
                except ClientConnectionError:
                    print('{} hat einen ConnectionError erzeugt.'.format(self.name))
                    #todo: insert new exception handling
                    self.exception_counter += 1
                    self.consecutive_exception = True
                except Exception as ex:
                    template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                    message = template.format(type(ex).__name__, ex.args)
                    print(message)
                    print('Die Response von {} konnte nicht gelesen werden.'.format(self.name))
                    #todo: insert new exception handling
                    self.exception_counter += 1
                    self.consecutive_exception = True

    def extract_request_urls(self, requests: dict) -> Dict[str, Tuple[str, Dict]]:
        """
        Helper-Method which should be only called by the constructor.
        Extracts from the section of requests from the .yaml-file
        the necessary attachment for the url and parameters for each request.

        api_url has to be initialized already.

        TODO: Possibility to use pair_template
        Example for one request:
            in bibox.yaml (request ticker):
                api_url: https://api.bibox.com/v1/
                template: mdata
                params:
                    cmd:
                        type: str
                        default: marketAll
            Result:
                url = https://api.bibox.com/v1/mdata
                params = {cmd: marketAll}

                in result dictionary:
                    {'ticker': [url, params], ...}
                    '...'  Means dictionary-entry for different request i.e. 'historic rates'.

        If 'params' contains the key-word "func", this method calls the corresponding
        function. The functions are defined in "utilities - REQUEST_PARAMS" as a dictionary.
        The executing method is named DatabaseHandler.request_params(dict{function, #params), params).
        The yaml-file needs to be written the following way:
                ....
                   params:
                     <parameter name>:
                       func:
                         <function name>
                         <func parameter>
                         <func parameter>
                         ...


        :param requests: Dict[str: Dict[param_name: value]]
            requests-section from a exchange.yaml as dictionary.
            Viability of dict is not checked.

        :return:
            See example above.
        """
        urls = dict()

        for request in requests:
            url = self.api_url
            request_dict = requests[request]['request']

            if 'template' in request_dict.keys() and request_dict['template']:
                url += '{}'.format(request_dict['template'])

            params = dict()
            if 'params' in request_dict.keys() and request_dict['params']:
                for param in request_dict['params']:
                    # extracts the function and assigns it to the method
                    if 'func' in request_dict['params'][param].keys():
                        params[param] = self.request_params(REQUEST_PARAMS[request_dict['params'][param]['func'][0]],
                                                            self.name,
                                                            *request_dict['params'][param]['func'][1:])
                    else:
                        params[param] = str(request_dict['params'][param]['default'])

            urls[request] = (url, params)

        return urls

    def extract_mappings(self, requests: dict) -> Dict[str, List[Mapping]]:
        """
        Helper-Method which should be only called by the constructor.
        Extracts out of a given exchange.yaml-requests-section for each
        request the necessary mappings so the values can be extracted from
        the response for said request.

        The key-value in the dictionary is the same as the key for the request.
        i.e. behind 'ticker' are all the mappings stored which are necessary for
        extracting the values out of a ticker-response.

        If there is no mapping specified in the .yaml for a value which is contained
        by the response, the value will not be extracted later on because there won't
        be a Mapping-object for said value.

        :param requests: Dict[str: List[Mapping]]
            Requests-section from a exchange.yaml as dictionary.
            Method does not check if dictionary contains viable information.

        :return:
            Dictionary with the following structure:
                {'request_name': List[Mapping]}
        """
        response_mappings = dict()
        for request in requests:
            request_mapping: dict = requests[request]

            if 'mapping' in request_mapping.keys():
                mapping = request_mapping['mapping']
                mapping_list = list()

                for entry in mapping:
                    mapping_list.append(Mapping(entry['key'], entry['path'], entry['type']))

                response_mappings[request] = mapping_list

        return response_mappings


    #[name, zeit, response.json]
    def format_ticker(self, response: Tuple[str, datetime, datetime, dict]) -> Iterator[Tuple[str,
                                                                                    datetime,
                                                                                    datetime,
                                                                                    str,
                                                                                    str,
                                                                                    float,
                                                                                    float,
                                                                                    float,
                                                                                    float,
                                                                                    float]]:

        """
        Extracts from the response-dictionary, with help of the suitable Mapping-Objects,
        the requested values and formats them to fitting tuples for persist_tickers() in db_handler.

        Starts with a dictionary of empty lists where each key is the possible key-name from a mapping.
        This is necessary because not every exchange returns every value that we try to store but a list
        is later necessary to fill up 'empty places'.
        i.e. Some exchanges don't return last_trade volumes for their currencies.

        TODO: prevent hardcoding key_names ( nice to have )

        Each Mapping stored behind response_mappings['ticker'] is then called to extract its values.
        i.e. The Mapping-Object with the key_name 'currency_pair_first' extracts a list which is ordered
        from first to last line with all the symbols of the first named currency.
        The return from extract_values() is then stored behind the key-name,
        e.g. the empty list is now replaced with the extracted values.

        The overall result of this process looks something like the following:
            first_currency =    ['BTC', 'ETH', ...]
            second_currency =   ['XRP', 'USDT', ...]
            ticker_last_price = []  <--- no Mapping-Object in exchange.yaml specified
            ticker_best_ask =   [0.123, 5.456, ...]
            ...
        Lists which are filled have the same length.
        (obviously because extraction of the same number of lines in response)
        The X-th elements from each value-list(extracted with Mapping-object) represent
        one 'response-line' in the given json.

        Because returning just the dictionary containing the lists is not intuitive
        the last step is formatting the extracted values into fitting tuples.
        This is done by using itertools.zip_longest() which works like the following:
            a = itertools.repeat('a', len(b))
            b = [1, 2, 3]
            c = []
            d = [1, 2]

            result:
                ('a', 1, None, 1)
                ('a', 2, None, 2)
                ('a', 3, None, None)

        We are using the length of currency_pair_first because every entry in general ticker_data
        has to contain a currency pair. It is always viable because the lists of extracted values
        all need to have the same length.

        The formatted list of ticker-data-tuples is then returned.

        AMEN

        :param response: Tuple[exchnage_name, time of arrival, response from exchnage-api]
            response is a parsed json -> Dict.

        :return: Iterator of tuples with the following structure:
                (exchange-name,
                 timestamp,
                 timestamp,
                 first_currency_symbol,
                 second_currency_symbol,
                 ticker_last_price,
                 ticker_last_trade,
                 ticker_best_ask,
                 ticker_best_bid,
                 ticker_daily_volume)

                Tuple might contain None if there was no Mapping-Object for a key(every x-th element of all
                 the tuples is none or the extracted value was simply None.
        """
        result = {'currency_pair_first': [],
                  'currency_pair_second': [],
                  'ticker_last_price': [],
                  'ticker_last_trade': [],
                  'ticker_best_ask': [],
                  'ticker_best_bid': [],
                  'ticker_daily_volume': []}

        mappings = self.response_mappings['ticker']
        for mapping in mappings:
            result[mapping.key] = mapping.extract_value(response[3])
          #  print(result)

        result = list(itertools.zip_longest(itertools.repeat(self.name,  len(result['currency_pair_first'])),
                                            itertools.repeat(response[1], len(result['currency_pair_first'])),
                                            itertools.repeat(response[2], len(result['currency_pair_first'])),
                                            result['currency_pair_first'],
                                            result['currency_pair_second'],
                                            result['ticker_last_price'],
                                            result['ticker_last_trade'],
                                            result['ticker_best_ask'],
                                            result['ticker_best_bid'],
                                            result['ticker_daily_volume']))
        return result

    def update_exception_counter(self):
        """
        This method updates the given parameter of the exception counter and the flag which represents the activity of
        the exchange.
        If the exception counter is greater than 3 the exchange will be set to passive.
        :return None
        """
        if self.exception_counter > 3:
            self.active_flag = False
            print('{} was set inactive.'.format(self.name))

    def update_consecutive_exception(self):
        """
        This method updates the given parameter of the consecutive exception bool which represents if an exchange throws
        consecutive exceptions.
        If the exceptions have not been thrown consecutive the counter will be reset to 0.
        :return None
        """
        if not self.consecutive_exception:
            self.exception_counter = 0
