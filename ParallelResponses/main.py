import asyncio
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List
# from csv_exporter import CsvExporter
from model.scheduling.Job import Job
from model.scheduling.scheduler import Scheduler
from model.database.db_handler import DatabaseHandler
from model.exchange.exchange import Exchange
from model.database.tables import metadata, ExchangeCurrencyPair
from model.utilities.utilities import read_config, yaml_loader, get_exchange_names
import signal

def signal_handler(signal, frame):
    """
    Helper function to exit the program. When strg+c is hit, the program will shut down with exit code(0)
    """
    print("\nExiting program.")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)


async def update_and_get_currency_pairs(exchange: Exchange, job_params: Dict):
    response = await exchange.request_currency_pairs('currency_pairs')

    if response[1] is not None:
        print('Updating Currency Pairs for {}...'.format(exchange.name.capitalize()))
        logging.info('Updating Currency Pairs for {}...'.format(exchange.name.upper()))
        formatted_response = exchange.format_currency_pairs(response)
        database_handler.persist_exchange_currency_pairs(formatted_response, is_exchange=exchange.is_exchange)

    return await get_currency_pairs(exchange, job_params)


async def get_currency_pairs(exchange: Exchange, job_params: Dict):
    print('Checking available currency pairs for {}...'.format(exchange.name.capitalize()),
          end=" ")
    logging.info('Checking available currency pairs.'.format(exchange.name.upper()))
    exchange_currency_pairs: List[ExchangeCurrencyPair] = database_handler.get_exchanges_currency_pairs(
        exchange.name,
        job_params['currency_pairs'],
        job_params['first_currencies'],
        job_params['second_currencies'])
    print('found {}'.format(len(exchange_currency_pairs)))
    return exchange_currency_pairs

async def initialize_jobs(database_handler: DatabaseHandler, job_config: Dict) -> List[Job]:
    jobs: [Job] = list()
    for job in job_config.keys():
        job_params: Dict = job_config[job]

        exchanges_with_pairs: [Exchange, List[ExchangeCurrencyPair]] = dict()
        exchange_names = job_params['exchanges'] if job_params['exchanges'][0] != 'all' else get_exchange_names()

        for exchange_name in exchange_names:
            # TODO: Error, wenn yaml nicht existiert
            exchange: Exchange = Exchange(yaml_loader(exchange_name))

            if job_params['update_cp'] is True:
                exchanges_with_pairs[exchange] = await update_and_get_currency_pairs(exchange, job_params)
            else:
                exchanges_with_pairs[exchange] = await get_currency_pairs(exchange, job_params)
                if not exchanges_with_pairs[exchange]:
                    exchanges_with_pairs[exchange] = await update_and_get_currency_pairs(exchange, job_params)
            if not exchanges_with_pairs[exchange]:
                del exchanges_with_pairs[exchange]



            print('Done loading currency pairs.', end="\n\n")
            logging.info('Done loading currency pairs.')

        if exchanges_with_pairs == {}:
            return None

        new_job: Job = Job(job,
                           job_params['yaml_request_name'],
                           exchanges_with_pairs)
        jobs.append(new_job)
    return jobs



async def main(database_handler: DatabaseHandler):
    """
    The model() function to run the program. Loads the database, including the database_handler.
    The exchange_names are extracted with a helper method in utilities based on existing yaml-files.
    In an asynchronous manner it is iterated over the exchange and and the responses are awaited and collected
        by await asyncio.gather(..)
    As soon as all responses from the exchange are returned, the values get extracted, formatted into tuples
        by the exchange.get_ticker(..) method and persisted by the into the database by the database_handler.
    """

    # run program with single exchange for debugging/testing purposes
    # exchange_names = ['binance']
    # TODO nicht vergessen config path zu ändern: gerade in hr_exchanges
    logging.info('Loading jobs.')
    jobs = await initialize_jobs(database_handler, read_config('jobs'))
    frequency = read_config('operation_settings')['frequency']
    logging.info('Configuring Scheduler.')
    scheduler = Scheduler(database_handler, jobs, frequency)
    print('{} were created and will run every {} minute(s).'.format(', '.join([job.name.capitalize() for job in jobs]), frequency))
    logging.info(
        '{} were created and will run every {} minute(s).'.format(', '.join([job.name.capitalize() for job in jobs]), frequency))

    while True:
        await scheduler.start()


def init_logger():
    if not read_config('utilities')['enable_logging']:
        logging.disable()
    else:
        if not os.path.exists('resources/log/'):
            os.makedirs('resources/log/')
        logging.basicConfig(filename='resources/log/{}.log'.format(datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')),
                            level=logging.INFO)


def handler(type, value, tb):
    logging.exception('Uncaught exception: {}'.format(str(value)))



if __name__ == "__main__":
    # todo: enable for exception in log
    sys.excepthook = handler
    init_logger()
    logging.info('Reading Database Configuration')
    db_params = read_config('database')
    logging.info('Establishing Database Connection')
    database_handler = DatabaseHandler(metadata, **db_params)
    asyncio.run(main(database_handler))

    # CsvExporter()
    # scheduler = Scheduler(database_handler, jobs)
    # scheduler.run()
