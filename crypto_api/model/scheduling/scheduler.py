import asyncio
import logging
import sys
from asyncio import Future
from typing import Callable, Any, Optional, Union, Coroutine

from model.database.db_handler import DatabaseHandler
from model.database.tables import Ticker, Trade, OrderBook, HistoricRate, ExchangeCurrencyPair
from model.exchange.exchange import Exchange
from model.scheduling.Job import Job
from model.utilities.exceptions import MappingNotFoundException
from model.utilities.time_helper import TimeHelper


class Scheduler:
    """
    The scheduler is in charge of requesting, filtering and persisting data received from the exchanges.
    Every x minutes the scheduler will be called to run the jobs created by the user in config.yaml.
    Attributes like frequency or job_list can also be set by the user in config.yaml.
    """

    def __init__(self, database_handler: DatabaseHandler, job_list: list[Job], frequency: Any):
        """
        Initializer for a Scheduler.

        @param database_handler: Handler that is called for everything that needs information from the database.
        @type database_handler: DatabaseHandler
        @param job_list: List of Jobs. A job can be created with the specific yaml-template in config.yaml.
        @type job_list: list[Job]
        @param frequency: The interval in minutes with that the run() method gets called.
        @type frequency: Any
        """
        self.database_handler = database_handler
        self.job_list = job_list
        self.frequency = frequency * 60 if isinstance(frequency, (int, float)) else frequency
        self.validated = False

    async def start(self):
        """
        Starts the process of requesting, filtering and persisting data for each job every x minutes.
        If a job takes longer than the frequency. The scheduler will wait until the job is finished
        and then start the jobs immediately again.
        Otherwise the scheduler will wait x minutes until it starts the jobs again.
        The interval begins counting down at the start of the current iteration.
        """
        runs: list[Union[Coroutine, Future]] = [self.run(job) for job in self.job_list]

        if isinstance(self.frequency, (int, float)):
            runs.append(asyncio.sleep(self.frequency))

        await asyncio.gather(*runs)

    async def run(self, job: Job):
        """
        The method represents one execution of the given job.

        @param job: The job that will be executed.
        @type job: Job
        """

        if not self.validated:
            await self.validate_job()

        request = self.determine_task(job.request_name)
        request_fun = request.get("function")
        request_table = request.get("table")

        continue_run = True
        while continue_run:
            continue_run, job.exchanges_with_pairs = await request_fun(request_table, job.exchanges_with_pairs)

    def determine_task(self, request_name: str) -> dict[str, Callable]:
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
                 "table": Trade},
        }

        return possible_requests.get(request_name, {
            "function": lambda: "Invalid request name.",
            "table": None
        })

    async def validate_job(self):
        """
        This methods validates the job_list given to the scheduler instance. If the job-list does not contain
        any "exchange_with_pairs" or no currency_pair for an exchange, the job is removed from the list.
        This happens of the user specified currency-pairs in the config but an exchange does not offer that pair.

        @return: New job_list without empty job and sets self.validated: True if the validation is successful.
        """

        self.job_list = await self.get_currency_pairs(self.job_list)
        self.remove_invalid_jobs(self.job_list)
        if self.job_list:
            self.validated = True

    def remove_invalid_jobs(self, jobs: list[Job]) -> list[Job]:
        """
        Method to clean up the job list. If the job list is empty, shut down program.
        Else the algorithm will go through every job specification and delete empty jobs or exchanges.

        @param jobs: List of all jobs specified by the config
        @type jobs: list[Job]

        @return: List of jobs, cleaned by empty or invalid jobs
        @rtype: list[Job]
        """
        if not jobs:
            logging.error("No or invalid Jobs.")

            print("\n No or invalid Jobs. This error occurs when the job list is empty due to no \n"
                  "matching currency pairs found for all exchanges. Please check your \n"
                  "parameters in the configuration.")
            sys.exit(0)

        for job in jobs:
            if job.request_name == "currency_pairs":
                print("Done loading Currency-Pairs.")
                sys.exit(0)

            # TODO: Philipp: Check out if loops work without continues.
            if job.exchanges_with_pairs:
                for exchange in job.exchanges_with_pairs.copy():
                    # Delete exchanges with no API for that request type
                    if job.request_name not in list(exchange.file["requests"].keys()):
                        job.exchanges_with_pairs.pop(exchange)
                        print(f"{exchange.name.capitalize()} has no {job.request_name} request method and was"
                              f" removed.")
                    # Delete exchanges with no matching Currency_Pairs
                    elif not job.exchanges_with_pairs[exchange]:
                        job.exchanges_with_pairs.pop(exchange)
                        print(f"{exchange.name.capitalize()} has no matching currency_pairs.")

                # Delete empty jobs, if the previous conditions removed all exchanges
                if not job.exchanges_with_pairs:
                    jobs.remove(job)

            else:
                # remove job if initially empty
                jobs.remove(job)

        if jobs:
            # If there are jobs left, return them
            return jobs
        else:
            # Reentry the method to get into the first else (down) condition and shut down process
            self.remove_invalid_jobs(jobs)

    # ToDo: Asynchronicity.
    async def get_currency_pairs(self, job_list: list[Job], *args) -> list[Job]:
        """
        Method to get all exchange currency pairs. First the database is queried, if the result is [], the exchanges
        api for all currency pairs is called.

        @param job_list: list of all jobs, including Job-Objects.
        @type job_list: list[Job]

        @return job_list with updated exchange_currency_pairs.
        @rtype: list[Job]
        """

        async def update_currency_pairs(ex: Exchange) -> list:
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
                    logging.exception(f"Error updating currency_pairs for {ex.name.capitalize()}")
                    return []
            else:
                return []

        for job in job_list:

            job_params = job.job_params
            exchanges = list(job.exchanges_with_pairs.keys())

            print("Loading and/or updating exchange currency pairs..")
            for exchange in exchanges:
                if job_params["update_cp"] or job.request_name == "currency_pairs" or \
                        not self.database_handler.get_all_currency_pairs_from_exchange(exchange.name):
                    await update_currency_pairs(exchange)

                if job.request_name != "currency_pairs":
                    job.exchanges_with_pairs[exchange] = self.database_handler.get_exchanges_currency_pairs(
                        exchange.name,
                        job_params["currency_pairs"],
                        job_params["first_currencies"],
                        job_params["second_currencies"]
                    )
        return job_list

    async def request_format_persist(self,
                                     request_table: object,
                                     exchanges_with_pairs: dict[Exchange, list[ExchangeCurrencyPair]]) \
            -> tuple[bool, dict[Exchange, list[ExchangeCurrencyPair]]]:
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
        @param exchanges_with_pairs: The exchanges including currency pairs to be queried and stored.
        @type exchanges_with_pairs: dict[Exchange, list[ExchangeCurrencyPair]]

        @return: A bool whether the job has to run again and a list of updated exchanges.
        @rtype: tuple[bool, dict[Exchange, list[ExchangeCurrencyPair]]]
        """
        table_name = request_table.__tablename__.capitalize()

        print(f"\nStarting to collect {table_name}.")
        logging.info(f"Starting to collect {table_name}.")

        start_time = TimeHelper.now()
        responses = await asyncio.gather(
            *(ex.request(request_table, exchanges_with_pairs[ex]) for ex in exchanges_with_pairs.keys())
        )
        counter = {}
        for response in responses:
            response_time = response[0]
            exchange_name = response[1]
            found_exchange: Optional[Exchange] = None

            for exchange in exchanges_with_pairs.keys():
                # finding the right exchange object
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
                    logging.exception(f"Exception formatting or persisting data for {found_exchange.name}")
                    continue

        if request_table.__name__ == "HistoricRate":
            updated_job: dict[Exchange, Any] = {}
            for exchange, value in counter.items():
                if value:
                    exchange.decrease_interval()
                    updated_job[exchange] = value
                elif exchange.interval != "days":  # if days had no results, kick out exchange
                    exchange.increase_interval()
                    updated_job[exchange] = value

            if updated_job:
                return True, updated_job

        print(f"Done collecting {table_name}.", end="\n\n")
        logging.info(f"Done collecting {table_name}.")

        return False, exchanges_with_pairs
