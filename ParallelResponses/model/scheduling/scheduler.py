import asyncio
import logging
import threading
import time
from datetime import datetime, timedelta
from typing import List, Callable, Dict

from model.scheduling.Job import Job

from model.database.db_handler import DatabaseHandler
from model.exchange.exchange import Exchange
from model.database.tables import ExchangeCurrencyPair, Ticker, HistoricRate, OrderBook, OHLCVM, Trade
import sys

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
        self.job_list = self.validate_job(job_list)
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

        # das hast du noch nachträglich eingefügt, vielleicht musst du das hier wieder ändern
        await asyncio.gather(*runs)

    async def run(self, job: Job):
        """
        The method represents one execution of the given job.
        @param job: Job
            The job that will be executed.

        """
        request = self.determine_task(job.request_name)
        request_fun = request.get('function')
        request_table = request.get('table')

        await request_fun(job.request_name, request_table, job.exchanges_with_pairs)

    def validate_job(self, job_list: List):
        """
        This methods validates the job_list given to the scheduler instance. If the job-list does not contain
        any "exchange_with_pairs", the job is removed from the list. This happens of the user specified currency-pairs
        in the config but an exchange does not offer that pair.

        :param job_list: List
            List with all jobs and required parameters.
        :return: job_list
            Returns the cleaned and validated job list.
        """
        if job_list:
            for job in job_list:
                if not job.exchanges_with_pairs:
                    job_list.remove(job)
            if job_list:
                return job_list
            else:
                self.validate_job(job_list)
        else:
            logging.error('No or invalid Jobs.')
            print("No or invalid Jobs. This error occurs when the job list is empty due to no \n"
                             "matching currency pairs found for a all exchanges. Please check your \n"
                             "parameters in the configuration.")
            sys.exit(0)


    def determine_task(self, request_name: str) -> Callable:
        """
        Returns the method that is to execute based on the given request name.

        @param request_name: str
            Name of the request.
        @return:
            Method for the request name or a string that the request is false.
        """
        possible_requests = {
            "currency_pairs":
                {'function': self.get_currency_pairs,
                 'table': ExchangeCurrencyPair},
            "ticker":
                {'function': self.get_job_done,
                 'table': Ticker},
            "historic_rates":
                {'function': self.get_job_done,
                 'table': HistoricRate},
            "order_books":
                {'function': self.get_job_done,
                 'table': OrderBook},
            "trades":
                {'function': self.get_job_done,
                 'table': Trade},
            "ohlcvm": {'function': self.get_job_done,
                       'table': OHLCVM}
        }
        return possible_requests.get(request_name, lambda: "Invalid request name.")

    async def get_currency_pairs(self, exchanges: Dict[str, Exchange], **kwargs):
        # ToDo wieso persisted diese methode keine Currency_pairs?!
        """
        Starts the currency pair request for each given exchange.

        @param exchanges:
            Exchanges that should send a request to their API for all trading
            currency pairs. Key is always the name of the exchange.
        @param kwargs: Currently unused parameters as the request name and the request table object.
        """
        print('Starting to collect currency pairs.')
        logging.info('Starting collection of currency pairs for all used exchanges.')
        responses = await asyncio.gather(
            *(exchanges[ex].request_currency_pairs('currency_pairs') for ex in exchanges))

        for response in responses:
            current_exchange = exchanges[response[0]]
            if response[1] is not None:
                currency_pairs = current_exchange.format_currency_pairs(response)
        # logging.info('Done collection currency pairs.\n')
        # print('Done collecting currency pairs.')

    async def get_job_done(self,
                           request_name: str,
                           request_table: object,
                           exchanges_with_pairs: Dict[Exchange, List[ExchangeCurrencyPair]]):
        #todo: doku
        print('Starting to collect {}.'.format(request_name.capitalize()), end="\n\n")
        logging.info('Starting to collect {}.'.format(request_name.capitalize()))
        start_time = datetime.utcnow()
        responses = await asyncio.gather(
            *(ex.request(request_name, exchanges_with_pairs[ex]) for ex in exchanges_with_pairs.keys())
        )

        for response in responses:
            if response:
                response_time = response[0]
                exchange_name = response[1]
            if response:
                found_exchange: Exchange = None
                for exchange in exchanges_with_pairs.keys():
                    # finding the right exchange
                    if exchange.name.upper() == exchange_name.upper():
                        found_exchange = exchange
                        break

                if found_exchange:
                    formatted_response, mappings = found_exchange.format_data(request_name,
                                                                              response[1:],
                                                                              start_time=start_time,
                                                                              time=response_time)

                    if formatted_response:
                        self.database_handler.persist_response(exchanges_with_pairs,
                                                               found_exchange,
                                                               request_name,
                                                               request_table,
                                                               formatted_response,
                                                               mappings)
        print('Done collecting {}.'.format(request_name.capitalize()), end="\n\n")
        logging.info('Done collecting {}.'.format(request_name.capitalize()))



# Currently unused or outdated methods:
#     async def get_tickers(self,
#     request_name,
#     request_table,
#     exchanges_with_pairs: Dict[Exchange, List[ExchangeCurrencyPair]]):
#         """
#         Tries to request, filter and persist ticker data for the given exchanges and their currency pairs.
#
#         @param exchanges_with_pairs:
#             Dictionary with all the exchanges and the currency pairs that need to be queried.
#         """
#         print('Starting to collect ticker.')
#         logging.info('Starting to collect ticker.')
#         start_time = datetime.utcnow()
#         responses = await asyncio.gather(
#             *(ex.request('ticker', exchanges_with_pairs[ex]) for ex in exchanges_with_pairs.keys()))
#
#         for response in responses:
#             if response:
#                 response_time = response[0]
#                 exchange_name = response[1]
#
#                 for exchange in exchanges_with_pairs.keys():
#                     if exchange.name.upper() == exchange_name.upper():
#                         break
#                 formatted_response, mappings = exchange.format_ticker(request_name,
#                                                                       response[1:],
#                                                                       start_time=start_time,
#                                                                       time=response_time)
#                 if formatted_response:
#                     self.database_handler.persist_response(exchanges_with_pairs,
#                                                            request_name,
#                                                            request_table,
#                                                            formatted_response,
#                                                            mappings)
#         logging.info('Done collecting ticker.')
#         print('Done collecting ticker.', end="\n\n")
