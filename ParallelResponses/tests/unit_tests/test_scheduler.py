import datetime
import unittest
from model.scheduling.scheduler import Scheduler
from model.scheduling.Job import Job
from model.database.db_handler import DatabaseHandler
from model.database.tables import *
from model.utilities.utilities import read_config

class TestMapping(unittest.TestCase):
    """Test class for Scheduling."""

    db_config = read_config('database')
    db_handler = DatabaseHandler(metadata, **db_config)

    # pylint: disable=too-many-public-methods

    def test_remove_invalid_exchanges_from_jobs(self):
        """Test of splitting a str and taking the index zero."""



        name = 'Ticker'
        exchanges = ['binance', 'poloniex']
        job_params = {'yaml_request_name': 'ticker'}
        exchanges_with_pairs = {'binance': ['BTC-USD', 'BTC-USDT'], 'poloniex': []}
        job = Job(name, job_params, exchanges_with_pairs)

        scheduler = Scheduler(self.db_handler, [job], 1)
        result = scheduler.remove_invalid_jobs([job])


        result = scheduler.job_list
        self.assertEqual(Job(name, job_params, {'binance': ['BTC-USD', 'BTC-USDT']}), result)
