import asyncio
import time
from datetime import datetime, timedelta
from typing import List, Callable, Dict

from Job import Job

import aioschedule as schedule

from db_handler import DatabaseHandler
from exchanges.exchange import Exchange
from tables import ExchangeCurrencyPair


class Scheduler:
    database_handler: DatabaseHandler
    job_list: List[Job]

    def __init__(self, database_handler: DatabaseHandler, job_list: List[Job]):
        self.database_handler = database_handler
        self.job_list = job_list

    def run(self):
        for job in self.job_list:
            exchanges: [Exchange] = job.exchanges
            request = self.determine_task(job.request_name)
            schedule.every(job.frequency).minutes.do(request, exchanges=exchanges)

        loop = asyncio.get_event_loop()
        while True:
            loop.run_until_complete(schedule.run_pending())
            time.sleep(0.1)

    def determine_task(self, request_name: str) -> Callable:
        possible_requests = {
            "ticker": self.get_tickers,
            "historic_rates": self.get_currency_pairs,
            "currency_pairs": self.get_currency_pairs
        }
        return possible_requests.get(request_name, lambda: "Invalid request name.")

    async def get_tickers(self, exchanges_with_pairs: Dict[Exchange, List[ExchangeCurrencyPair]]):
        """
        All exchanges will be managed in two lists ( a primary and a secondary list ). Every exchange will be separated
        each request-run depending of its activity flag ( look exchange.py class description ). All exchanges in the
        primary list will send ticker requests. All exchanges in the secondary list will their connection.
        :param exchanges_with_pairs: The dictionary of all given exchanges with the given exchange currency pairs
        :return: None
        """
        print('Starting to collect ticker.')
        # checking every exchange for its flag
        primary_exchanges = {}
        secondary_exchanges = {}
        #todo: loop wieder einfügen für hochfrequente daten
        for exchange in exchanges_with_pairs.keys():
            if exchange.active_flag:
                primary_exchanges[exchange] = exchanges_with_pairs[exchange]
            else:
                secondary_exchanges[exchange] = exchanges_with_pairs[exchange]

        # start_time : datetime when request run is started
        # delta : given microseconds for the datetime
        start_time = datetime.utcnow()
        delta = start_time.microsecond
        # rounding the given datetime on seconds
        start_time = start_time - timedelta(microseconds=delta)
        if delta >= 500000:
            start_time = start_time + timedelta(seconds=1)

        # if there are exchanges to request, one request per exchange will be sent
        if not len(primary_exchanges) == 0:
            responses = await asyncio.gather(*(ex.request('ticker', start_time) for ex in primary_exchanges.keys()))

            for response in responses:
                if response:
                    # print('Response: {}'.format(response))
                    exchange_name = response[0]
                    for exchange in primary_exchanges.keys():
                        if exchange.name.upper() == exchange_name.upper():
                            break
                    formatted_response = exchange.format_ticker(response)
                    self.database_handler.persist_tickers(primary_exchanges[exchange], formatted_response)
        else:
            print('There are currently no exchanges to request')

        # if there are exchanges to test the connection, one test per exchange will be sent
        if not len(secondary_exchanges) == 0:
            test_responses = await asyncio.gather(*(ex.test_connection() for ex in secondary_exchanges.keys()))
            for test_response in test_responses:
                if test_response:
                    # print('Test result: {}'.format(test_response))
                    exchange = test_response[0]
                    exchange.update_flag(test_response[1])
        else:
            print('There are currently no exchanges to test its connection.')

        print('Done collecting ticker.')

    async def get_historic_rates(self, exchanges: [Exchange]):
        print('Starting to collect historic rates.')
        for ex in exchanges:
            curr_exchange: Exchange = exchanges[ex]

            # Setting Currency-Pairs
            all_currency_pairs: [ExchangeCurrencyPair] = self.database_handler.get_all_exchange_currency_pairs(
                curr_exchange.name)
            curr_exchange.exchange_currency_pairs = all_currency_pairs

            # Getting Historic Rates
            hr_response = await curr_exchange.request_historic_rates('historic_rates',
                                                                     curr_exchange.exchange_currency_pairs)
            if hr_response is not None:
                formatted_hr_response = curr_exchange.format_historic_rates(hr_response)
                self.database_handler.persist_historic_rates(formatted_hr_response)

        print('Done collecting historic rates.')

    async def get_currency_pairs(self, exchanges: [Exchange]):
        print('Starting to collect currency pairs.')
        responses = await asyncio.gather(*(ex.request_currency_pairs('currency_pairs') for ex in exchanges))

        for response in responses:
            current_exchange = exchanges[response[0]]#todo list or dict, wenn list umbauen.
            if response[1] is not None:
                currency_pairs = current_exchange.format_currency_pairs(response)
                self.database_handler.persist_exchange_currency_pairs(currency_pairs)

        print('Done collecting currency pairs.')
