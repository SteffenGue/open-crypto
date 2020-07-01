import asyncio
import time
from datetime import datetime
from typing import List, Callable

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
            schedule.every(job.frequency).minute.do(request, exchanges=exchanges)

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

    async def get_tickers(self, exchanges: [Exchange]):
        print('Starting to collect ticker.')
        start_time = datetime.utcnow()
        responses = await asyncio.gather(*(exchanges[ex].request('ticker', start_time) for ex in exchanges))

        # todo: remove
        for ex in exchanges:
            self.database_handler.get_currency_pairs_with_first_currency(exchanges[ex].name, 'btc')
            self.database_handler.get_currency_pairs_with_second_currency(exchanges[ex].name, 'btc')

        for response in responses:
            if response:
                # print('Response: {}'.format(response))
                exchange = exchanges[response[0]]
                formatted_response = exchange.format_ticker(response)
                self.database_handler.persist_tickers(formatted_response)
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
        responses = await asyncio.gather(
            *(exchanges[ex].request_currency_pairs('currency_pairs') for ex in exchanges))

        for response in responses:
            current_exchange = exchanges[response[0]]
            if response[1] is not None:
                currency_pairs = current_exchange.format_currency_pairs(response)
                self.database_handler.persist_exchange_currency_pairs(currency_pairs)

        print('Done collecting currency pairs.')
