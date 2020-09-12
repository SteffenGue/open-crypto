import asyncio
import logging
import threading
from datetime import datetime
from typing import List, Callable, Dict

from model.scheduling.Job import Job

from model.database.db_handler import DatabaseHandler
from model.exchange.exchange import Exchange
from model.database.tables import ExchangeCurrencyPair


class Scheduler:
    database_handler: DatabaseHandler
    job_list: List[Job]
    frequency: int

    def __init__(self, database_handler: DatabaseHandler, job_list: List[Job], frequency: int):
        self.database_handler = database_handler
        self.job_list = job_list
        self.frequency = frequency * 60

    async def start(self):
        runs = [self.run(job) for job in self.job_list]
        runs.append(asyncio.sleep(self.frequency))

        await asyncio.gather(*runs)

    async def run(self, job: Job):
        request = self.determine_task(job.request_name)
        await request(job.exchanges_with_pairs)

    def determine_task(self, request_name: str) -> Callable:
        possible_requests = {
            "ticker": self.get_tickers,
            "historic_rates": self.get_currency_pairs,
            "currency_pairs": self.get_currency_pairs
        }
        return possible_requests.get(request_name, lambda: "Invalid request name.")

    async def get_tickers(self, exchanges_with_pairs: Dict[Exchange, List[ExchangeCurrencyPair]]):
        print('Starting to collect ticker.')
        logging.info('Starting Ticker collection.')
        start_time = datetime.utcnow()
        responses = await asyncio.gather(
            *(ex.request('ticker', start_time, exchanges_with_pairs[ex]) for ex in exchanges_with_pairs.keys()))

        added_ticker_counter = 0
        for response in responses:
            if response:
                # print('Response: {}'.format(response))
                exchange_name = response[0]
                for exchange in exchanges_with_pairs.keys():
                    if exchange.name.upper() == exchange_name.upper():
                        break
                formatted_response = exchange.format_ticker(response)
                if formatted_response:
                    added_ticker_counter += self.database_handler.persist_tickers(exchanges_with_pairs[exchange], formatted_response)
        logging.info('Done collecting ticker.')
        logging.info('Added {} Ticker tuple to the database.\n'.format(added_ticker_counter))
        print('Done collecting ticker.')

    async def get_historic_rates(self, exchanges: [Exchange]):
        # todo: funktioniert noch nicht. methode existiert nur aufgrund von refactoring
        print('Starting to collect historic rates.')
        logging.info('Starting to collect historic rates.')
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
        logging.info('Done collecting historic rates.\n')

    async def get_currency_pairs(self, exchanges: Dict[str, Exchange]):
        """
        Starts the currency pair request for each given exchange.

        @param exchanges:
            Exchanges that should send a request to their API for all trading
            currency pairs. Key is always the name of the exchange.
        """
        print('Starting to collect currency pairs.')
        logging.info('Starting collection of currency pairs for all used exchanges.')
        responses = await asyncio.gather(
            *(exchanges[ex].request_currency_pairs('currency_pairs') for ex in exchanges))

        for response in responses:
            current_exchange = exchanges[response[0]]
            if response[1] is not None:
                currency_pairs = current_exchange.format_currency_pairs(response)
        logging.info('Done collection currency pairs.\n')
        print('Done collecting currency pairs.')
