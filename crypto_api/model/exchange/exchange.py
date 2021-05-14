import itertools
import logging
import traceback
import string
from datetime import datetime
from typing import Iterator, Dict, List, Tuple, Optional, Any
import aiohttp
import asyncio
import tqdm
from collections import deque, OrderedDict
from aiohttp import ClientConnectionError

from model.exchange.Mapping import Mapping, convert_type
from model.database.tables import ExchangeCurrencyPair
from model.utilities.exceptions import MappingNotFoundException, DifferentExchangeContentException, \
    NoCurrencyPairProvidedException
from model.utilities.time_helper import TimeHelper


def extract_mappings(exchange_name: str, requests: dict) -> dict[str, list[Mapping]]:
    """
    Helper-Method which should be only called by the constructor.
    Extracts out of a given exchange .yaml-requests-section for each
    request the necessary mappings so the values can be extracted from
    the response for said request.

    The key-value in the dictionary is the same as the key for the request.
    i.e. behind 'ticker' are all the mappings stored which are necessary for
    extracting the values out of a ticker-response.

    If there is no mapping specified in the .yaml for a value which is contained
    by the response, the value will not be extracted later on because there won't
    be a Mapping-object for said value.

    @param exchange_name: str
        String representation of the exchange name.
    @param requests: Dict[str: List[Mapping]]
        Requests-section from a exchange.yaml as dictionary.
        Method does not check if dictionary contains viable information.

    @return:
        Dictionary with the following structure:
            {'request_name': List[Mapping]}
    """
    response_mappings = dict()
    if requests:
        for request in requests:
            request_mapping: dict = requests[request]

            if 'mapping' in request_mapping.keys():
                mapping = request_mapping['mapping']
                mapping_list = list()

                try:
                    for entry in mapping:
                        mapping_list.append(Mapping(entry['key'], entry['path'], entry['type']))
                except KeyError:
                    print(f"Error loading mappings of {exchange_name} in {request}: {entry}")
                    logging.error(f"Error loading mappings of {exchange_name} in {request}: {entry}")
                    break

                response_mappings[request] = mapping_list

    return response_mappings


def format_request_url(url: str, pair_template: dict, pair_formatted: str, cp, parameter: dict):
    # ToDo: Docu
    """
    @param cp:
    @param pair_template:
    @param url:
    @param pair_formatted:
    @param parameter:
    @return:
    """

    parameter.update({key: parameter[key][cp] for key, val in parameter.items() if isinstance(val, dict)})

    # Case 1: Currency-Pairs in request parameters: eg. www.test.com?market=BTC-USD
    if 'alias' in pair_template.keys() and pair_template['alias']:
        # add market=BTC-USD to parameters
        parameter[pair_template['alias']] = pair_formatted
        # params_adj = parameter
        # url_formatted = url

    # Case 2: Currency-Pairs directly in URL: eg. www.test.com/BTC-USD
    elif pair_formatted:
        parameter.update({'currency_pair': pair_formatted})

    else:
        return url, parameter
        # find placeholders in string

    variables = [item[1] for item in string.Formatter().parse(url) if item[1] is not None]
    url_formatted = url.format(**parameter)
    # drop params who got filled directly into the url
    parameter = {k: v for k, v in parameter.items() if k not in variables}

    return url_formatted, parameter


class Exchange:
    """
    Attributes:
        # ToDo: Docu check
        Embodies the characteristics of a crypto-currency-exchange.
        Each exchange is an Exchange-object.
        Each 'job' is in the end a method called on every exchange.

        The Attributes and mappings are all extracted from the
        .yaml-files whose location is described in the trades.yaml file.

        name: str
            Name of this exchange.
        timeout: int
            Timeout for every request before throwing an exception
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
        exception_counter: int
            Integer which counts the exceptions thrown by this exchange.
        consecutive_exception: bool
            Boolean which represents if the exceptions has been thrown consecutive.
        active_flag: bool
            Boolean which represents if this exchange is active or passive. If an exchange will throw three exception
            consecutive, it will be set to passive and will no longer be requested.
    """
    name: str
    is_exchange: bool
    api_url: str
    rate_limit: float
    interval: Any
    request_urls: dict
    response_mappings: dict
    exception_counter: int
    consecutive_exception: bool
    active_flag: bool
    timeout: int
    exchange_currency_pairs: List[ExchangeCurrencyPair]

    def __init__(self, yaml_file: Dict, db_last_timestamp, timeout, interval: Any = "days"):
        """
        Creates a new Exchange-object.

        Checks the content of the .yaml if it contains certain keys.
        If searched keys exist, the constructor sets the values for the described attributes.
        The constructor also calls extract_request_urls() and extract_mappings()
        to create request_urls and the necessary Mapping-Objects.

        :param yaml_file: Dict
            Content of a .yaml file as a dict-object.
            Constructor does not check if content is viable.
        """
        self.file = yaml_file
        self.timeout = timeout
        self.name = yaml_file["name"]
        self.interval = interval
        self.base_interval = interval
        self.get_first_timestamp = db_last_timestamp

        self.api_url = yaml_file["api_url"]
        if yaml_file.get("rate_limit"):
            if yaml_file["rate_limit"]["max"] <= 0:
                self.rate_limit = 0
            else:
                self.rate_limit = yaml_file["rate_limit"]["unit"] / yaml_file["rate_limit"]["max"]
        else:
            self.rate_limit = 0

        self.response_mappings = extract_mappings(self.name, yaml_file["requests"])

        self.exception_counter = 0
        self.active_flag = True
        self.consecutive_exception = False
        self.is_exchange = yaml_file.get("exchange")
        self.exchange_currency_pairs = list()

    def add_exchange_currency_pairs(self, currency_pairs: list):
        """
        Method that adds the given currency-pairs to exchange-currency-pairs.
        TODO: check if contains is enough to prevent duplicates of pairs
        @param currency_pairs:
            Pairs that should be added to exchange_currency_pairs.
        """
        for cp in currency_pairs:
            if cp not in self.exchange_currency_pairs:
                self.exchange_currency_pairs.append(cp)

    async def test_connection(self) -> Tuple[str, bool, Dict]:
        """
        This method sends either a connectivity test ( like a ping call or a call which sends the exchange server time )
        or, if no calls like this are available or exist in the public web api, a ticker request will be send.
        Exceptions will be caught and in this case the connectivity test failed.

        The Method is asynchronous so that after the request is send, the program does not wait
        until the response arrives. For asynchronously we use the library asyncio.
        For sending and dealing with requests/responses the library aiohttp is used.

        The methods gets the requests matching url out of request_urls.
        If it does not exist None will be returned. Otherwise it sends and awaits the response(.json).

        :return: (str, bool)
            Tuple of the following structure:
                (exchange_name, response)
                response represents the result of the connectivity test
        :exceptions ClientConnectionError: the connection to the exchange timed out or the exchange did not answered
                    Exception: the given response of an exchange could not be evaluated
        """
        if self.request_urls.get("test_connection"):
            async with aiohttp.ClientSession() as session:
                request_url_and_params = self.request_urls["test_connection"]
                try:
                    response = await session.get(request_url_and_params[0], params=request_url_and_params[1])
                    response_json = await response.json(content_type=None)
                    return self.name, True, response_json
                except ClientConnectionError:
                    return self.name, False, {}
                except Exception:
                    return self.name, False, {}

    async def request(self,
                      request_table: object,
                      currency_pairs: List[ExchangeCurrencyPair]) -> \
            Optional[Tuple[datetime, str, Dict[Optional[ExchangeCurrencyPair], Any]]]:

        """
        Method tries to request data for all given methods and currency pairs.
        Depending on if data can be received for all available currency pairs with one request
        the methods sends a request for each currency pair or just one request for all the data.

        The Method is asynchronous so that after the request is send, the program does not wait
        until the response arrives. For asynchronicity we use the library asyncio.
        For sending and dealing with requests/responses the library aiohttp is used.

        The methods gets the requests matching url out of request_urls.
        Parameters are passed in a dictionary.
        If it does not exist None will be returned. Otherwise it sends and awaits the response(.json)
        and tries afterwards to parse the json to a dictionary.
        The parsed json is then returned with the name of this exchange, so
        the response is assignable to this exchange and the time when the response arrived.

        Exceptions will be caught and a suitable message is printed.
        None will be returned in this case.

        TODO: Good Exception handling
        TODO: Logging von Exceptions / option in config
        TODO: Saving responses / option in config

        @param request_table: object
            Object of the request table. i.e. 'Ticker' for tickers-request
        @param currency_pairs:
            List of currency pairs that should be requested.

        @return: (str, datetime, datetime, .json)
            Tuple of the following structure:
                (exchange_name, start time, response time, response)
                - time of arrival is a datetime-object in utc
        @exceptions ClientConnectionError: the connection to the exchange timed out or the exchange did not answered
                    Exception: the given response of an exchange could not be evaluated
        """
        request_name = request_table.__tablename__

        self.request_urls = self.extract_request_urls(self.file['requests'][request_name],
                                                      request_name=request_name,
                                                      request_table=request_table,
                                                      currency_pairs=currency_pairs)

        if request_name in self.request_urls.keys() and self.request_urls[request_name]:

            request_url_and_params = self.request_urls[request_name]
            responses = dict()
            params = request_url_and_params['params']
            pair_template_dict = request_url_and_params['pair_template']
            url: str = request_url_and_params['url']

            self.rate_limit = self.rate_limit if self.rate_limit and len(currency_pairs) >= self.rate_limit else 0

            # when there is no pair formatting section then all ticker data can be accessed with one request
            if pair_template_dict:
                pair_formatted = {cp: self.apply_currency_pair_format(request_name, cp) for cp in currency_pairs}

            async with aiohttp.ClientSession() as session:
                for cp in tqdm.tqdm(currency_pairs, disable=(len(currency_pairs) < 1000)):

                    # # if formatted currency pair needs to be a parameter
                    # if 'alias' in pair_template_dict.keys() and pair_template_dict['alias']:
                    #     params[pair_template_dict['alias']] = pair_formatted[cp]
                    #     url_formatted = url
                    # elif pair_template_dict:
                    #     url_formatted = url.format(currency_pair=pair_formatted[cp])
                    # else:
                    #     url_formatted = url

                    # ToDO: Test method format_request_url
                    if pair_template_dict:
                        url_formatted, params_adj = format_request_url(url,
                                                                       pair_template_dict,
                                                                       pair_formatted[cp],
                                                                       cp,
                                                                       params)
                    else:
                        url_formatted, params_adj = url, params

                    try:
                        response = await session.get(url=url_formatted,
                                                     params=params_adj,
                                                     timeout=aiohttp.ClientTimeout(total=self.timeout))
                        # ToDo: Does every successful response has code 200?
                        assert (response.status == 200)
                        # Changes here: deleted await response.json(...) as it throw a ClientPayloadError for extrates.
                        # However the response.status was 200 and could be persisted into the DB...
                        response_json = await response.json(content_type=None)
                        if pair_template_dict:
                            responses[cp] = response_json
                        else:  # when ticker data is returned for all available currency pairs at once
                            responses[None] = response_json
                            break

                    except (ClientConnectionError, asyncio.TimeoutError):
                        print('No connection to {}. Timeout or ConnectionError!'.format(self.name.capitalize()))
                        logging.error('No connection to {}. Timeout or ConnectionError!'.format(self.name.capitalize()))
                        # ToDo: Changes for all exception: "return -> responses[cp]= []"
                        responses[cp] = []
                    except AssertionError:
                        print(
                            "Failed request for {}: {}. Status {}.".format(self.name.capitalize(), cp, response.status))
                        responses[cp] = []
                    except Exception as e:
                        print('Unable to read response from {}. Check exchange config file.\n'
                              'Url: {}, Parameters: {}'
                              .format(self.name, url_formatted, request_url_and_params['params']))
                        print(e)

                        logging.error('Unable to read response from {}. Check config file.\n'
                                      'Url: {}, Parameters: {}'
                                      .format(self.name, url_formatted, params))
                        responses[cp] = []

                    await asyncio.sleep(self.rate_limit)

            return TimeHelper.now(), self.name, responses
        else:
            logging.warning('{} has no Ticker request. Check {}.yaml if it should.'.format(self.name, self.name))
            print("{} has no {} request.".format(self.name, request_name))
            return None

    def apply_currency_pair_format(self, request_name: str, currency_pair: ExchangeCurrencyPair) -> str:
        """
        Helper method that applies the format described in the yaml for the specific request on the given currency-pair.
        Does currently not test if the given name with the needed parameters exist in request_urls.

        @param request_name:
            Key for the request_urls dict. Is the name of the request in the yaml for this exchange.
            So something like "historic_rates", "ticker" ...
        @param currency_pair:
            Currency-pair that needs to be formatted.
        @return:
            String of the formatted currency-pair.
            Example: BTC and ETH -> "btc_eth"
        """
        request_url_and_params: Dict = self.request_urls[request_name]
        pair_template_dict = request_url_and_params['pair_template']
        pair_template = pair_template_dict['template']

        formatted_string: str = pair_template.format(first=currency_pair.first.name, second=currency_pair.second.name)

        if pair_template_dict['lower_case']:
            formatted_string = formatted_string.lower()

        return formatted_string

    async def request_currency_pairs(self, request_name: str = 'currency_pairs') -> Tuple[str, Dict]:
        """
        Tries to retrieve all available currency-pairs that are traded on this exchange.

        @param request_name:
            Key for the request_urls dict. Is the name of the request in the yaml for this exchange.
            Should be named "currency_pairs" in the provided yaml-files.
        @return Tuple[str, Dict]:
            Returns a tuple containing the name of this exchange and the response from the Rest-API.
            Dict might be None if an error occurred during the request or the request_name
            does not exist or is empty in the yaml.
        """

        self.request_urls = self.extract_request_urls(self.file['requests'][request_name],
                                                      request_name=request_name)
        response_json = None
        if request_name in self.request_urls.keys() and self.request_urls[request_name]:
            async with aiohttp.ClientSession() as session:
                request_url_and_params = self.request_urls[request_name]
                try:
                    response = await session.get(request_url_and_params['url'],
                                                 params=request_url_and_params['params'],
                                                 timeout=aiohttp.ClientTimeout(total=self.timeout))
                    response_json = await response.json(content_type=None)

                    assert (response.status == 200)

                except (ClientConnectionError, asyncio.TimeoutError):
                    print('No connection to {}. Timeout- or ConnectionError!'.format(self.name.capitalize()))
                    self.exception_counter += 1
                    return self.name, None

                except AssertionError:
                    print("Failed request for {}. Status {}.".format(self.name.capitalize(), response.status))
                    return self.name, None

                except Exception:
                    print('Unable to read response from {}. Check exchange config file.\n'
                          'Url: {}, Parameters: {}'
                          .format(self.name, request_url_and_params['url'], request_url_and_params['params']))
                    logging.warning('Unable to read response from {}. Check config file.\n'
                                    'Url: {}, Parameters: {}'
                                    .format(self.name, request_url_and_params['url'], request_url_and_params['params']))
                    self.exception_counter += 1
                    return self.name, None
        else:
            logging.warning('{} has no currency pair request. Check {}.yaml if it should.'.format(self.name, self.name))
            print("{} has no currency-pair request.".format(self.name))
        return self.name, response_json

    def extract_request_urls(self,
                             request_dict: dict,
                             request_name: str,
                             request_table: object = None,
                             currency_pairs: List[ExchangeCurrencyPair] = None) -> Dict[str, Dict[str, Dict]]:
        # ToDo: Doku der Variables
        # ToDo: Doku Update mit variablen request parametern.
        """
        Helper-Method which should be only called by the constructor.
        Extracts from the section of requests from the .yaml-file
        the necessary attachment for the url and parameters for each request.

        api_url has to be initialized already.

        Example for one request:
            in bibox.yaml (request ticker):
                api_url: https://api.bibox.com/v1/
                requests:
                    ticker:
                        request:
                            template: mdata?cmd=marketAll
                            pair_template: null
                            params: null

            Result:
                request_urls = {'ticker': ('https://api.bibox.com/v1/mdata?cmd=marketAll', {}, {}) )}

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

        @param request_name: str representation of the request method
        @param currency_pairs: list of all exchange-currency-pairs
        @param request_table: object of the database table
        @param request_dict: Dict[str: Dict[param_name: value]] requests-section from the exchange.yaml
        @return: dict of request url, pair template and parameters.
            See example above.
        """
        request_dict = request_dict['request']

        urls = dict()
        request_parameters = dict()
        request_parameters['url'] = self.api_url + request_dict.get('template', "")
        request_parameters['pair_template'] = request_dict.get('pair_template', None)

        params = dict()
        parameters = request_dict.get('params', False)
        if not parameters:
            request_parameters['params'] = {}
            urls[request_name] = request_parameters
            return urls

        def allowed(val: dict, **kwargs):
            """
            Extract the configured value from all allowed values. If there is no match, return str "default".
            @param val: dict of allowed key, value pairs.
            @return: value if key in dict, else None.
            """
            value = val.get(self.interval, None)
            # in order to change the Class interval to the later used default value. The KEY is needed, therefore
            # is the dict-comprehension {v: k for k, v ...}.
            if not bool(value):
                self.interval = {v: k for k, v in val.items()}
            return value

        def function(val: str, **kwargs) -> Any:
            """
            Execute function for all currency-pairs. Function returns the first timestamp in the DB, or
            datetime.now() if none exists.
            @param kwargs: not used but needed for another function.
            @param val: contains the function name as string.
            @return:
            """
            if val == 'last_timestamp':
                return {cp: self.get_first_timestamp(request_table, cp.id) for cp in currency_pairs}

        def default(val: str, **kwargs) -> str:
            """
            Returns the default value if kwargs value (the parameter) is None.
            @param val: Default value.
            @param kwargs: Parameter value. If None, return default value.
            @return: Default value as a string.
            """
            default_val = val if not bool(kwargs.get('has_value')) else kwargs.get('has_value')
            if isinstance(self.interval, dict):
                self.interval = self.interval.get(default_val)
                self.base_interval = self.interval
            return default_val

        def type(val, **kwargs):
            """
            Performs type conversions.
            @param val: The conversion values specified under "type".
            @param kwargs: The value to be converted.
            @return: Converted value.
            """
            param_value = kwargs.get("has_value", None)
            conv_params = val
            # to avoid conversion when only a type declaration was done. If a parameter is of type "int".
            if isinstance(conv_params, str) or len(conv_params) < 2:
                return param_value
            # replace the key "interval" with the interval specified in the configuration file.
            conv_params = [self.interval if x == 'interval' else x for x in conv_params]
            # return {cp: convert_type(param_value[cp], deque(conv_params)) for cp in currency_pairs}
            # ToDo: Check if the above line works. The older version needed both if statements below.
            if isinstance(param_value, dict):
                return {cp: convert_type(param_value[cp], deque(conv_params)) for cp in currency_pairs}
            elif isinstance(conv_params, list):
                return convert_type(param_value, deque(conv_params))

        mapping: dict = {'allowed': allowed, 'function': function, 'default': default, 'type': type}
        # enumerate mapping dict to sort parameter values accordingly.
        mapping_index = {val: key for key, val in enumerate(mapping.keys())}

        for param, options in parameters.items():
            # Kick out all option keys which are not in the mapping dict or where required: False.
            # Sort the dict options according to the mapping to ensure the right order of function calls.
            options = {k: v for k,v in options.items() if k in mapping.keys()}
            options = OrderedDict(sorted(options.items(), key=lambda x: mapping_index.get(x[0])))
            if not parameters[param].get('required', True):
                continue
            # Iterate over the functions and fill the params dict with values. Kwargs are needed only partially.
            kwargs = {'has_value': None}
            for key, val in options.items():
                kwargs.update({'has_value': params.get(param, None)})
                params[param] = mapping.get(key)(val, **kwargs)

        request_parameters['params'] = params
        urls[request_name] = request_parameters
        return urls

    def format_currency_pairs(self, response: Tuple[str, Dict]) -> Iterator[Tuple[str, str, str]]:
        # ToDo: Error Handling
        """
        Extracts the currency-pairs of out of the given json-response
        that was collected from the Rest-API of this exchange.

        Process is similar to @see{self.format_ticker()}.

        @param response:
            Raw json-response from the Rest-API of this exchange that needs be formatted.
        @return:
            Iterator containing tuples of the following structure:
            (self.name, name of first currency-pair, name of second currency-pair)
        """
        if response[0] != self.name:
            return None

        results = {'currency_pair_first': [],
                   'currency_pair_second': []}
        mappings = self.response_mappings['currency_pairs']

        for mapping in mappings:
            results[mapping.key] = mapping.extract_value(response[1])

            if isinstance(results[mapping.key], str):
                # If the result is only one currency, it will be split into every letter.
                # To avoid this, put it into a list.
                results[mapping.key] = [results[mapping.key]]

        assert (len(results[0]) == len(result) for result in results)
        len_results = {key: len(value) for key, value in results.items() if hasattr(value, '__iter__')}
        len_results = max(len_results.values()) if bool(len_results) else 1
        results.update({k: itertools.repeat(*v, len_results) for k, v in results.items() if len(v) == 1})

        return list(itertools.zip_longest(itertools.repeat(self.name, len(results['currency_pair_first'])),
                                          results['currency_pair_first'],
                                          results['currency_pair_second']))

    def format_data(self,
                    method: str,
                    response: Tuple[str, Dict[object, Dict]],
                    start_time: datetime,
                    time: datetime):

        """
        Extracts from the response-dictionary, with help of the suitable Mapping-Objects,
        the requested values and formats them to fitting tuples for persist_response() in db_handler.

        Starts with a dictionary of empty lists where each key is the possible key-name from a mapping.
        This is necessary because not every exchange returns every value that we try to store but a list
        is later necessary to fill up 'empty places'.
        i.e. Some exchange don't return trade_volumes for their currencies.

        Each Mapping stored behind response_mappings[method] is then called to extract its' values.
        i.e. The Mapping-Object with the key_name 'currency_pair_first' extracts a list which is ordered
        from first to last line with all the symbols of the first named currency.
        The return from extract_values() is then stored behind the key-name,
        e.g. the empty list is now replaced with the extracted values.

        The overall result of this process looks something like the following:
            first_currency =    ['BTC', 'ETH', ...]
            second_currency =   ['XRP', 'USD', ...]
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

        @param method: str
            The request method name, i.e. ticker, trades,...
        @param response: Iterator
            response is a parsed json -> Dict.
                Tuple might contain None if there was no Mapping-Object for a key (every x-th element of all
                 the tuples is none or the extracted value was simply None.
        @param start_time: datetime
            Timestamp when the request started.
        @param time: datetime
            Timestamp the exchange responded.

        @return List of Tuples with the formatted data and the name of the mapping_keys to map the data points.
        """

        if response[0] != self.name:
            raise DifferentExchangeContentException(response[0], self.name)
        results = list()
        if method in self.response_mappings.keys():
            mappings = self.response_mappings[method]
            if not mappings:
                raise MappingNotFoundException(self.name, method)
        else:
            raise MappingNotFoundException(self.name, method)
        responses = response[1]
        currency_pair: ExchangeCurrencyPair
        mapping_keys = [mapping.key for mapping in mappings]

        # creating dictionary where key is the name of the mapping which holds an empty list
        temp_results = dict(zip((key for key in mapping_keys),
                                itertools.repeat([], len(mappings))))

        for currency_pair in responses.keys():
            if currency_pair:  # responses had to be collected individually
                current_response = responses[currency_pair]
            else:  # data for all currency_pairs in one response
                current_response = responses[None]

            if current_response:
                try:
                    # extraction of actual values; note that currencies might not be in mappings (later important)
                    for mapping in mappings:
                        if currency_pair:
                            temp_results[mapping.key]: List = mapping.extract_value(current_response,
                                                                                    currency_pair_info=(
                                                                                        currency_pair.first.name,
                                                                                        currency_pair.second.name,
                                                                                        self.apply_currency_pair_format(
                                                                                            method,
                                                                                            currency_pair)))
                        else:
                            temp_results[mapping.key]: List = mapping.extract_value(current_response)

                        if isinstance(temp_results[mapping.key], str):
                            # Bugfix: if value is a single string, it is an iterable, and the string will
                            # be split in every letter. Therefore it is put into a list.
                            temp_results[mapping.key] = [temp_results[mapping.key]]

                except Exception:
                    print('Error while formatting {}, {}: {}'.format(method, mapping.key, currency_pair))
                    traceback.print_exc()
                    pass
                else:
                    # extracted_data_is_valid: bool = True
                    # for extracted_field in temp_results.keys():
                    #     if temp_results[extracted_field] is None:
                    #         print("{} has no valid data in {}".format(currency_pair, extracted_field))
                    # extracted_data_is_valid = False
                    # continue

                    # CHANGE: One filed invalid -> all fields invalid.
                    # changed this in order to avoid responses kicked out just because of one invalid field.
                    # The response will be filtered out in the DB-Handler if the primary-keys are missing anyways.
                    if all(value is None for value in list(temp_results.values()) if not isinstance(value, datetime)):
                        continue

                    # asserting that the extracted lists for each mapping are having the same length
                    assert (len(results[0]) == len(result) for result in temp_results)

                    len_results = {key: len(value) for key, value in temp_results.items() if hasattr(value, '__iter__')}
                    len_results = max(len_results.values()) if bool(len_results) else 1

                    if (method == 'order_books') and ('position' in temp_results.keys()):
                        # Sort the order_books by price. I.e. asks ascending, Bids descending.
                        bids = [(price, amount) for (price, amount) in
                                sorted(zip(temp_results['bids_price'], temp_results['bids_amount']),
                                       reverse=True,
                                       key=lambda pair: pair[0])]
                        asks = [(price, amount) for (price, amount) in
                                sorted(zip(temp_results['asks_price'], temp_results['asks_amount']),
                                       reverse=False,
                                       key=lambda pair: pair[0])]

                        temp_results.update({'bids_price': [bid[0] for bid in bids]})
                        temp_results.update({'bids_amount': [bid[1] for bid in bids]})
                        temp_results.update({'asks_price': [ask[0] for ask in asks]})
                        temp_results.update({'asks_amount': [ask[1] for ask in asks]})

                        # Implement the order-book position for easy query afterwards.
                        temp_results['position'] = range(len_results)

                    # adding pair id when we don't have currencies in mapping
                    if 'currency_pair_first' not in mapping_keys and 'currency_pair_second' not in mapping_keys:
                        if currency_pair:
                            temp_results.update({'exchange_pair_id': currency_pair.id})
                        else:
                            raise NoCurrencyPairProvidedException(self.name, method)
                    elif 'currency_pair_first' not in mapping_keys or 'currency_pair_second' not in mapping_keys:
                        raise NoCurrencyPairProvidedException(self.name, method)

                    # update new keys only if not already exists to prevent overwriting!
                    temp_results = {'start_time': start_time, 'time': time, **temp_results}
                    result = [v if hasattr(v, '__iter__')
                              else itertools.repeat(v, len_results) for k, v in temp_results.items()]

                    result = list(itertools.zip_longest(*result))
                    results.extend(result)
        return results, list(temp_results.keys())

    def increase_interval(self):
        index = self.interval_strings.index(self.interval) + 1
        index = min(index, len(self.interval_strings) - 1)

        self.interval = self.interval_strings[index]

    def decrease_interval(self):
        # TODO: Not lower than base?
        if self.interval is None or self.interval == self.base_interval:
            return

        index = self.interval_strings.index(self.interval) - 1
        index = max(index, 0)

        self.interval = self.interval_strings[index]

    interval_strings = ["seconds", "minutes", "hours", "days"]
