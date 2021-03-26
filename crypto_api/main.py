import asyncio
import logging
import os
import signal
import sys
from datetime import datetime
from typing import Dict, List

from model.database.db_handler import DatabaseHandler
from model.database.tables import metadata, ExchangeCurrencyPair
from model.exchange.exchange import Exchange
from model.scheduling.Job import Job
from model.scheduling.scheduler import Scheduler
from model.utilities.utilities import read_config, yaml_loader, get_exchange_names


def signal_handler(signal, frame):
    """
    Helper function to exit the program. When CTRL+C is hit, the program will shut down with exit code(0).
    """
    print("\nExiting program.")
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)


async def initialize_jobs(job_config: Dict, timeout, interval, db_handler: DatabaseHandler) -> List[Job]:
    """
    Initializes and creates new Job Objects and stores them in a list. There will be one Job-Object for every request
    method, independent of the amount of exchanges or currency_pairs specified in the config. The Dict
    'exchanges_with_pairs' is created with Exchange Objects as keys, the values are filled in the Scheduler.
    @param db_handler: Instance of the DatabaseHandler to pass to the Exchange Class. This is to be able to
                        perform database queries for variable request parameters.
    @param timeout: Request timeout for the Exchange Class.
    @param interval: Request Interval for HistoricRate (i.e. Daily, Minutes,..)
    @param job_config: Dictionary with job parameter gathered from the config-file.
    @return: A list of Job objects.
    """
    jobs: [Job] = list()

    for job in job_config.keys():
        job_params: Dict = job_config[job]
        exchange_names = job_params['exchanges'] if job_params['exchanges'][0] != 'all' else get_exchange_names()

        if job_params.get('excluded'):
            exchange_names = [item for item in exchange_names if item not in job_params.get('excluded', [])]

        exchange_names = [yaml_loader(exchange) for exchange in exchange_names if yaml_loader(exchange) is not None]

        exchanges: [Exchange] = [Exchange(exchange_name,
                                          db_handler.get_first_timestamp,
                                          timeout,
                                          interval=interval) for exchange_name in exchange_names]

        exchanges_with_pairs: [Exchange, List[ExchangeCurrencyPair]] = dict.fromkeys(exchanges)

        new_job: Job = Job(job, job_params, exchanges_with_pairs)
        jobs.append(new_job)

    return jobs


def init_logger(path):
    if not read_config(file=None, section='utilities')['enable_logging']:
        logging.disable()
    else:
        if not os.path.exists(path + '/resources/log/'):
            os.makedirs('resources/log/')
        logging.basicConfig(
            filename=path + f'/resources/log/{datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")}.log',
            level=logging.ERROR)


def handler(ex_type, ex_value, ex_traceback):
    logging.exception(f'Uncaught exception: {str(ex_type)}: {str(ex_value)}')


async def main(database_handler: DatabaseHandler):
    """
    The model() function to run the program. Loads the database, including the database_handler.
    The exchange_names are extracted with a helper method in utilities based on existing yaml-files.
    In an asynchronous manner it is iterated over the exchange and and the responses are awaited and collected
        by await asyncio.gather(..)
    As soon as all responses from the exchange are returned, the values get extracted, formatted into tuples
        by the exchange.get_ticker(..) method and persisted by the into the database by the database_handler.
    """
    config = read_config(file=None, section=None)

    logging.info('Loading jobs.')
    jobs = await initialize_jobs(job_config=config['jobs'],
                                 timeout=config['general']['operation_settings']['timeout'],
                                 interval=config['general']['operation_settings']['interval'],
                                 db_handler=database_handler)
    frequency = config['general']['operation_settings']['frequency']
    logging.info('Configuring Scheduler.')
    scheduler = Scheduler(database_handler, jobs, frequency)
    await scheduler.validate_job()

    desc = f'{", ".join([job.name.capitalize() for job in jobs])} were created and will run every {frequency} minute(s).'

    print(desc)
    logging.info(desc)

    while True:
        if frequency == 'once':
            loop = asyncio.get_event_loop()
            try:
                loop.run_until_complete(await scheduler.start())
            except RuntimeError:
                sys.exit(0)
        else:
            try:
                await scheduler.start()
            except Exception as e:
                logging.exception(datetime.utcnow(), e)
                pass


def run(path: str = None):
    sys.excepthook = handler
    init_logger(path)

    logging.info('Reading Database Configuration')
    db_params = read_config(file=None, section='database')

    logging.info('Establishing Database Connection')
    database_handler = DatabaseHandler(metadata, path=path, **db_params)

    # Windows Bug I don't really understand. See Github Issue:
    # https: // github.com / encode / httpx / issues / 914
    if sys.version_info[0] == 3 and sys.version_info[1] >= 8 and sys.platform.startswith('win'):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # ToDo: diese Methode genauso verlässlich? -> Try Except um scheduler.start()
    asyncio.run(main(database_handler))
    # while True:
    #     try:
    #         asyncio.run(main(database_handler))
    #     except Exception:
    #         logging.exception("Restarting Program. at {}".format(datetime.utcnow()))
    #         pass
