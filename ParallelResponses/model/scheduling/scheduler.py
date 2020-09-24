import asyncio
import logging
import threading
import time
from datetime import datetime, timedelta
from typing import List, Callable, Dict

from model.scheduling.Job import Job

from model.database.db_handler import DatabaseHandler
from model.exchange.exchange import Exchange
from model.database.tables import ExchangeCurrencyPair


class Scheduler:
    """
    The scheduler is in charge of requesting, filtering and persisting data received from the exchanges.
    Every x minutes the scheduler will be called to run the jobs created by the user in config.yaml.
    Attributes like frequency or job_list can also be set by the user in config.yaml.
    """
    database_handler: DatabaseHandler
    job_list: List[Job]
    frequency: int

    def __init__(self, database_handler: DatabaseHandler, job_list: List[Job], frequency: int):
        """
        Initializer for a Scheduler.

        @param database_handler: DatabaseHandler
            Handler that is called for everything that needs information from the database.
        @param job_list: List[Job]
            List of Jobs. A job can be created with the specific yaml-template in config.yaml
        @param frequency: int
            The interval in minutes with that the run() method gets called.
        """
        self.database_handler = database_handler
        self.job_list = job_list
        self.frequency = frequency * 60

    async def start(self):
        """
        Starts the process of requesting, filtering and persisting data for each job every x minutes.
        If a job takes longer than the frequency. The scheduler will wait until the job is finished
        and then start the jobs immediately again.
        Otherwise the scheduler will wait x minutes until it starts the jobs again.
        The interval begins counting down at the start of the current iteration.
        """
        runs = [self.run(job) for job in self.job_list]
        runs.append(asyncio.sleep(self.frequency))

        #das hast du noch nachträglich eingefügt, vielleicht musst du das hier wieder ändern
        await asyncio.gather(*runs)

    async def run(self, job: Job):
        """
        The method represents one execution of the given job.
        @param job: Job
            The job that will be executed.

        """
        request = self.determine_task(job.request_name)
        await request(job.exchanges_with_pairs)

    def determine_task(self, request_name: str) -> Callable:
        """
        Returns the method that is to execute based on the given request name.

        @param request_name: str
            Name of the request.
        @return:
            Method for the request name or a string that the request is false.
        """
        possible_requests = {
            "ticker": self.get_tickers,
            "historic_rates": self.get_historic_rates,
            "order_books": self.get_order_books,
            "currency_pairs": self.get_currency_pairs
        }
        return possible_requests.get(request_name, lambda: "Invalid request name.")

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

    async def get_tickers(self, exchanges_with_pairs: Dict[Exchange, List[ExchangeCurrencyPair]]):
        """
        Tries to request, filter and persist ticker data for the given exchanges and their currency pairs.

        @param exchanges_with_pairs:
            Dictionary with all the exchanges and the currency pairs that need to be queried.
        """
        print('Starting to collect ticker.')
        logging.info('Starting to collect ticker.')
        start_time = datetime.utcnow()
        responses = await asyncio.gather(
            *(ex.request_tickers('ticker', start_time, exchanges_with_pairs[ex]) for ex in exchanges_with_pairs.keys()))

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


    async def get_historic_rates(self, exchanges_with_pairs: Dict[Exchange, List[ExchangeCurrencyPair]]):
        # todo: funktioniert noch nicht. methode existiert nur aufgrund von refactoring
        print('Starting to collect historic rates.')
        logging.info('Starting to collect historic rates.')

        responses = await asyncio.gather(
            *(ex.request('historic_rates', exchanges_with_pairs[ex]) for ex in exchanges_with_pairs.keys()))

        added_tuple_counter = 0
        for response in responses:
            if response:
                # print('Response: {}'.format(response))
                exchange_name = response[0]
                for exchange in exchanges_with_pairs.keys():
                    if exchange.name.upper() == exchange_name.upper():
                        break
                formatted_response = exchange.format_historic_rates(response)

                if formatted_response:
                    added_tuple_counter += self.database_handler.persist_historic_rates(exchange_name,
                                                                                        formatted_response)

        print('Done collecting historic rates.')
        # print('Added {} Ticker tuple to the database.\n'.format(added_tuple_counter))
        logging.info('Done collecting historic rates.\n')
        # logging.info('Added {} Ticker tuple to the database.\n'.format(added_tuple_counter))


    async def get_order_books(self, exchanges_with_pairs: Dict[Exchange, List[ExchangeCurrencyPair]]):
        """
         Tries to request, filter and persist order-book data for the given exchanges and their currency pairs.

         @param exchanges_with_pairs:
             Dictionary with all the exchanges and the currency pairs that need to be queried.
         """

        print('Starting to collect order books.')
        logging.info('Starting to collect order books')

        responses = await asyncio.gather(
            *(ex.request('order_books', exchanges_with_pairs[ex]) for ex in exchanges_with_pairs.keys())
        )

        added_tuple_counter = 0

        for response in responses:
            if response:
                exchange_name = response[0]

                for exchange in exchanges_with_pairs.keys():
                    if exchange.name.upper == exchange_name.upper():
                        break
                formatted_response = exchange.format_order_books(response)

                if formatted_response:
                    added_tuple_counter += self.database_handler.persist_order_books(exchange_name, formatted_response)

        print('Done collecting order books.')
        logging.info('Done collecting order books.')
