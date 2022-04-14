#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Module to schedule the program. The scheduler receives a job list, validates it and calls the methods
to request, extract and persist data asynchronously. The scheduler is programmed to request several currency-pairs
at once, however it can be changed to an vertical requesting (i.e. on currency-pair at a time). This may be useful
for a high amount of currency-pairs, to avoid filling the RAM.
"""

import asyncio
import logging
from asyncio import Future
from typing import Callable, Any, Optional, Union, Coroutine, List, Dict, Tuple

from model.database.db_handler import DatabaseHandler
from model.database.tables import Ticker, Trade, OrderBook, HistoricRate, ExchangeCurrencyPair, DatabaseTable
from model.exchange.exchange import Exchange
from model.scheduling.job import Job
from model.utilities.exceptions import MappingNotFoundException
from model.utilities.kill_switch import KillSwitch
from model.utilities.loading_bar import Loader
from model.utilities.time_helper import TimeHelper


class Scheduler:
    """
    The scheduler is in charge of requesting, filtering and persisting data received from the exchanges.
    Every x minutes the scheduler will be called to run the jobs created by the user in config.yaml.
    Attributes like frequency or job_list can also be set by the user in config.yaml.
    """

    def __init__(self, database_handler: DatabaseHandler, job_list: List[Job],
                 asynchronicity: Union[int, bool], frequency: Union[str, int, float]):
        """
        Initializer for a Scheduler.

        @param database_handler: Handler that is called for everything that needs information from the database.
        @type database_handler: DatabaseHandler
        @param job_list: List of Jobs. A job can be created with the specific yaml-template in config.yaml.
        @type job_list: list[Job]
        @param asynchronicity: Specifies the requesting method, horizontal or vertical.
        @param frequency: The interval in minutes with that the run() method gets called.
        @type frequency: Any
        """
        self.database_handler = database_handler
        self.job_list = job_list
        self.asynchronicity = asynchronicity
        self.frequency = frequency * 60 if isinstance(frequency, (int, float)) else frequency
        self._validated = False

    async def start(self) -> None:
        """
        Starts the process of requesting, filtering and persisting data for each job every x minutes.
        If a job takes longer than the frequency. The scheduler will wait until the job is finished
        and then start the jobs immediately again.
        Otherwise, the scheduler will wait x minutes until it starts the jobs again.
        The interval begins counting down at the start of the current iteration.
        """
        runs: list[Union[Coroutine[Any, Any, None], Future[Any]]] = [self.run(job) for job in self.job_list]

        if isinstance(self.frequency, (int, float)):
            runs.append(asyncio.sleep(self.frequency))
        await asyncio.gather(*runs)

    async def run(self, job: Job) -> None:
        """
        The method represents one execution of the given job.

        @param job: The job that will be executed.
        @type job: Job
        """

        if not self._validated:
            await self.validate_job()

        request = self.determine_task(job.request_name)
        request_fun = request.get("function")
        request_table = request.get("table")

        if self.asynchronicity is False:
            for exchange, currency_pairs in job.exchanges_with_pairs.items():
                for currency_pair, last_row_id in currency_pairs.items():
                    continue_run = True
                    while continue_run:
                        if KillSwitch().stay_alive is False:
                            print("\nTask got terminated.")
                            logging.info("Task got terminated.")
                            break
                        last_row_id = job.exchanges_with_pairs.get(exchange).get(currency_pair)
                        continue_run, job.exchanges_with_pairs = \
                            await request_fun(request_table, {exchange: {currency_pair: last_row_id}})
                        if not continue_run:
                            continue

        else:
            continue_run = True
            while continue_run:
                if KillSwitch().stay_alive is False:
                    print("\nTask got terminated.")
                    logging.info("Task got terminated.")
                    break
                continue_run, job.exchanges_with_pairs = await request_fun(request_table, job.exchanges_with_pairs)

        print("Terminating.")

    def determine_task(self, request_name: str) -> Dict[str, Union[Callable[..., None]]]:
        """
        Returns the method that is to execute based on the given request name.

        @param request_name: Name of the request.
        @type request_name: str

        @return: Method for the request name or a string that the request is false.
        @rtype: dict[str, Callable]
        """

        possible_requests = {
            "currency_pairs":
                {"function": self.get_currency_pairs,
                 "table": ExchangeCurrencyPair},
            "tickers":
                {"function": self.request_format_persist,
                 "table": Ticker},
            "historic_rates":
                {"function": self.request_format_persist,
                 "table": HistoricRate},
            "order_books":
                {"function": self.request_format_persist,
                 "table": OrderBook},
            "trades":
                {"function": self.request_format_persist,
                 "table": Trade}
        }

        return possible_requests.get(request_name, {
            "function": lambda: "Invalid request name.",
            "table": None
        })

    async def validate_job(self) -> None:
        """
        This method validates the job_list given to the scheduler instance. If the job-list does not contain
        any "exchange_with_pairs" or no currency_pair for an exchange, the job is removed from the list.
        This happens of the user specified currency-pairs in the config but an exchange does not offer that pair.

        @return: New job_list without empty job and sets self.validated: True if the validation is successful.
        """

        self.job_list = await self.get_currency_pairs(self.job_list)
        self.remove_invalid_jobs(self.job_list)
        if self.job_list:
            self._validated = True

    def remove_invalid_jobs(self, jobs: List[Job]) -> List[Job]:
        """
        Method to clean up the job list. If the job list is empty, shut down program.
        Else the algorithm will go through every job specification and delete empty jobs or exchanges.

        @param jobs: List of all jobs specified by the config
        @type jobs: list[Job]
        @return: List of jobs, cleaned by empty or invalid jobs
        @rtype: list[Job]
        """
        if not jobs:
            logging.error("No or invalid Job(s).")

            print("\nNo currency-pair(s) found for the specified exchange(s). "
                  "Please check your request settings in the configuration file.")
            raise SystemExit

        for job in jobs:
            if job.request_name == "currency_pairs":
                print("\nDone loading Currency-Pairs.")
                raise SystemExit

            if job.exchanges_with_pairs:
                for exchange in job.exchanges_with_pairs.copy():
                    # Delete exchanges with no API for that request type
                    if job.request_name not in list(exchange.file["requests"].keys()):
                        job.exchanges_with_pairs.pop(exchange)
                        logging.info("%s has no %s request method and was removed.",
                                     exchange.name.capitalize(), job.request_name)
                    # Delete exchanges with no matching Currency_Pairs
                    elif not job.exchanges_with_pairs[exchange]:
                        job.exchanges_with_pairs.pop(exchange)
                        logging.info("%s has no matching currency_pairs.", exchange.name.capitalize())

                # Delete empty jobs, if the previous conditions removed all exchanges
                if not job.exchanges_with_pairs:
                    jobs.remove(job)

            else:
                # remove job if initially empty
                jobs.remove(job)

        if jobs:
            # If there are jobs left, return them
            for job in jobs:
                print(f"Requesting {len(job.exchanges_with_pairs.keys())} exchange(s) for job: {job.name}.")
            return jobs
        # Reenter the method to get into the first else (down) condition and shut down process
        self.remove_invalid_jobs(jobs)

    async def update_currency_pairs(self, ex: Exchange) -> List[None]:
        """
        This method requests the currency_pairs.

        @param ex: Current exchange object.
        @type ex: Exchange

        @return: Empty list if no response from the exchange.
        @rtype: list
        """
        response = await ex.request_currency_pairs()
        if response[1]:
            try:
                formatted_response = ex.format_currency_pairs(response)
                self.database_handler.persist_exchange_currency_pairs(formatted_response,
                                                                      is_exchange=ex.is_exchange)
            except (MappingNotFoundException, TypeError, KeyError):
                logging.exception("Error updating currency_pairs for %s", ex.name.capitalize())
                return []
        else:
            return []

    async def get_currency_pairs(self, job_list: List[Job]) -> List[Job]:
        """
        Method to get all exchange currency pairs. First the database is queried, if the result is [], the exchanges
        api for all currency pairs is called.

        @param job_list: list of all jobs, including Job-Objects.
        @type job_list: list[Job]

        @return job_list with updated exchange_currency_pairs.
        @rtype: list[Job]
        """
        loader: Loader

        for job in job_list:
            job_params = job.job_params
            exchanges = list(job.exchanges_with_pairs.keys())

            logging.info("Loading and/or updating exchange currency pairs..")

            with Loader("Checking exchange currency-pairs...", "", max_counter=len(exchanges)) as loader:
                exchanges_to_update = list()
                for exchange in exchanges:
                    if job_params["update_cp"] or job.request_name == "currency_pairs" or \
                            not self.database_handler.get_all_currency_pairs_from_exchange(exchange.name):
                        exchanges_to_update.append(self.update_currency_pairs(exchange))
                    loader.increment()

            with Loader("Requesting exchange currency-pairs...", "", max_counter=len(exchanges_to_update)) as loader:
                for exchange in exchanges_to_update:
                    await exchange
                    loader.increment()

            with Loader("Loading exchange currency-pairs...", "", max_counter=len(exchanges)) as loader:
                for exchange in exchanges:
                    if job.request_name != "currency_pairs":
                        job.exchanges_with_pairs[exchange] = dict.fromkeys(
                            self.database_handler.get_exchanges_currency_pairs(
                                exchange.name,
                                job_params["currency_pairs"],
                                job_params["first_currencies"],
                                job_params["second_currencies"]
                            ))
                    loader.increment()

        return job_list

    async def request_format_persist(self,
                                     request_table: DatabaseTable,
                                     exchanges_with_pairs: Dict[Exchange, Dict[ExchangeCurrencyPair, None]]) \
            -> Tuple[bool, Dict[Exchange, Dict[ExchangeCurrencyPair, None]]]:
        """"
        Gets the job done. The request are sent concurrently and awaited. Afterwards the responses
        are formatted via "found_exchange.format_data()", a method from the Exchange Class. The formatted
        responses and the mappings (IMPORTANT: THE ORDER OF THE RESPONSE TUPLES AND MAPPINGS REMAIN UNTOUCHED)
        are given to the DatabaseHandler where they are checked, single items removed (who do not belong in the
        database table, especially the start_time is only present in the ticker table) and persisted.

        This method works for all kind of request, except the currency_pairs. To add a new request-type
        (i.e. like order_books, ticker,..) add a new item into 'possible_requests' from self.determine_task(),
        create a new database class (i.e. like OrderBook, Ticker,...) and expand the yaml-file for each exchange.

        Please ensure the following:
            - The database columns MUST match the mapping-keys from the yaml-file.
            - The order of the mapping-keys from the yaml-file does not matter. It is matched to the values
                in "Exchange.format_data()" and handed over to SQLAlchemy (where a new object is created for each row)
                via **kwargs.
            - The DatabaseHandler will reject to persist new items if any primary key is emtpy.
            - For more detailed instructions, including an example, see into the handbook.

        @param request_table: The database table storing the data.
        @type request_table: object
        @param exchanges_with_pairs: The exchanges including currency pairs to be queried. The value of the dict
                                    contains the last row_id of the last insert. Useful to retrieve the next timestamp
                                    for HR requests.
        @type exchanges_with_pairs: dict[Exchange, Dict[ExchangeCurrencyPair, Optional[int]]

        @return: A bool whether the job has to run again and a list of updated exchanges.
        @rtype: tuple[bool, dict[Exchange, list[ExchangeCurrencyPair]]]
        """
        table_name = request_table.__tablename__.capitalize()

        logging.info("Starting to request %s.", table_name)

        start_time = TimeHelper.now()

        total = sum([len(v) for v in exchanges_with_pairs.values()])

        loader: Loader
        with Loader("Requesting data...", "", max_counter=total) as loader:
            responses = await asyncio.gather(
                *(ex.request(request_table, exchanges_with_pairs[ex], loader=loader) for ex in
                  exchanges_with_pairs.keys())
            )

        counter = {}

        for response in responses:
            if not response:
                continue

            response_time = response[0]
            exchange_name = response[1]
            found_exchange: Optional[Exchange] = None

            for exchange in exchanges_with_pairs.keys():
                # Find the right exchange object
                if exchange.name.upper() == exchange_name.upper():
                    found_exchange = exchange
                    break

            if found_exchange:
                try:
                    formatted_response = found_exchange.format_data(request_table.__tablename__,
                                                                    response[1:],
                                                                    start_time=start_time,
                                                                    time=response_time)

                    if formatted_response:
                        counter[found_exchange] = self.database_handler.persist_response(exchanges_with_pairs,
                                                                                         found_exchange,
                                                                                         request_table,
                                                                                         formatted_response)

                except (MappingNotFoundException, TypeError, KeyError):
                    logging.exception("Exception formatting or persisting data for %s", found_exchange.name)
                    continue

        if request_table.__name__ == "HistoricRate":
            updated_job: Dict[Exchange, Any] = {}
            for exchange, value in counter.items():
                if value:
                    updated_job[exchange] = value

            if updated_job:
                return True, updated_job

        logging.info("Done collecting %s.", table_name)
        return False, exchanges_with_pairs
