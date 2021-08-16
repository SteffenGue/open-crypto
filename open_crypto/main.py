#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Module initializes the jobs, starts the logger and finally the asyncio event-loop. Furthermore the
signal_handler adds the possibility to interrupt the process, the handler function to catch and log unexpected errors.
"""

import asyncio
import logging
import signal
import sys
from typing import Any

from model.utilities.patch_event_loop import PatchEventLoop
from model.database.db_handler import DatabaseHandler
from model.database.tables import metadata, ExchangeCurrencyPair
from model.exchange.exchange import Exchange
from model.scheduling.job import Job
from model.scheduling.scheduler import Scheduler
from model.utilities.time_helper import TimeHelper
from model.utilities.utilities import read_config, yaml_loader, get_exchange_names, load_program_config
from model.utilities.utilities import signal_handler, init_logger
from model.utilities.loading_bar import Loader
from model.utilities.kill_switch import KillSwitch

signal.signal(signal.SIGINT, signal_handler)


async def initialize_jobs(job_config: dict[str, Any],
                          timeout: int,
                          interval: Any,
                          comparator: str,
                          db_handler: DatabaseHandler) -> list[Job]:
    """
    Initializes and creates new Job Objects and stores them in a list. There will be one Job-Object for every request
    method, independent of the amount of exchanges or currency_pairs specified in the config. The Dict
    'exchanges_with_pairs' is created with Exchange Objects as keys, the values are filled in the Scheduler.
    @param db_handler: Instance of the DatabaseHandler to pass to the Exchange Class. This is to be able to
                        perform database queries for variable request parameters.
    @param timeout: Request timeout for the Exchange Class.
    @param interval: Request Interval for HistoricRate (i.e. Daily, Minutes,..)
    @param comparator: Defines if the request interval of exchanges must match, be smaller or equal, ect.
    @param job_config: Dictionary with job parameter gathered from the config-file.
    @return: A list of Job objects.
    """
    jobs: list[Job] = list()

    for job in job_config.keys():
        job_params = job_config[job]
        exchange_names = job_params["exchanges"] if job_params["exchanges"][0] != "all" else get_exchange_names()

        if job_params.get("excluded"):
            exchange_names = [item for item in exchange_names if item not in job_params.get("excluded", [])]

        exchange_names = [yaml_loader(exchange) for exchange in exchange_names if yaml_loader(exchange) is not None]

        exchanges: [Exchange] = [Exchange(exchange_name,
                                          db_handler.get_first_timestamp,
                                          timeout,
                                          comparator=comparator,
                                          interval=interval) for exchange_name in exchange_names]

        exchanges_with_pairs: [Exchange, list[ExchangeCurrencyPair]] = dict.fromkeys(exchanges)

        new_job: Job = Job(job, job_params, exchanges_with_pairs)
        jobs.append(new_job)

    return jobs


async def main(database_handler: DatabaseHandler, program_config: dict) -> Scheduler.start:
    """
    The model() function to run the program. Loads the database, including the database_handler.
    The exchange_names are extracted with a helper method in utilities based on existing yaml-files.
    In an asynchronous manner it is iterated over the exchange and the responses are awaited and collected
        by await asyncio.gather(..)
    As soon as all responses from the exchange are returned, the values get extracted, formatted into tuples
        by the exchange.get_ticker(..) method and persisted by the into the database by the database_handler.

    @param: Instance of the DatabaseHandler
    @type database_handler: object
    @param program_config: Additional advanced program settings
    @type program_config: dict
    """

    with Loader("Initializing open_crypto...", ""):
        config = read_config(file=None, section=None)
        logging.info("Loading jobs.")
        jobs = await initialize_jobs(job_config=config["jobs"],
                                     timeout=config["general"]["operation_settings"].get("timeout", 10),
                                     interval=config["general"]["operation_settings"].get("interval", "days"),
                                     comparator=program_config['request_settings'].get('interval_settings',
                                                                                       'lower_or_equal'),
                                     db_handler=database_handler)
        frequency = config["general"]["operation_settings"]["frequency"]
        logging.info("Configuring Scheduler.")
        scheduler = Scheduler(database_handler,
                              jobs,
                              program_config.get('request_settings').get('request_direction', 0),
                              frequency)
    await scheduler.validate_job()

    desc = f"Job(s) were created and will run with frequency: {frequency}."
    logging.info(desc)

    while True:
        if not KillSwitch().stay_alive:
            print("Task got terminated.")
            logging.info("Task got terminated.")
            break

        if frequency == "once":
            loop = asyncio.get_event_loop()
            try:
                loop.run_until_complete(await scheduler.start())
                # This part below the loop will never be arrived as the scheduler will raise an RuntimeError
                # as soon as the job_list is empty.
            except (RuntimeError, TypeError) as exc:
                raise SystemExit from exc

        else:
            try:
                await scheduler.start()
            except Exception as ex:
                logging.exception(TimeHelper.now(), ex)


def run(file: str = None, path: str = None) -> None:
    """
    Starts the program and initializes the asyncio-event-loop.
    @param file:
    @param path: String representation to the current working directory or any PATH specified in runner.py
    """

    # sys.excepthook = handler

    program_config = load_program_config()
    db_params = read_config(file=file, section="database")
    init_logger(path, program_config)

    logging.info("Establishing Database Connection")
    database_handler = DatabaseHandler(metadata,
                                       path=path,
                                       min_return_tuples=program_config["request_settings"].get("min_return_tuples", 2),
                                       **db_params)

    # See Github Issue for bug and work-around:
    # https://github.com/encode/httpx/issues/914
    if sys.version_info[0] == 3 and sys.version_info[1] >= 8 and sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    if PatchEventLoop.check_event_loop_exists():
        PatchEventLoop.apply_patch()

    asyncio.run(main(database_handler, program_config))
