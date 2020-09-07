import csv
import os
from datetime import datetime
from typing import Dict, List, Tuple

from model.database.db_handler import DatabaseHandler
import model.utilities.utilities as utilities
from model.database.tables import metadata
from dateutil import parser as dateparser


class CsvExporter:
    database_handler: DatabaseHandler

    # export parameter
    path: str
    name: str
    delimiter: str

    # query options
    query_everything: bool
    from_timestamp: datetime
    to_timestamp: datetime
    exchanges: List[str]
    first_currencies: List[str]
    second_currencies: List[str]
    currency_pairs: List[Tuple[str, str]]

    def __init__(self):
        db_params: Dict = utilities.read_config('database', 'csv_config.yaml')
        self.database_handler = DatabaseHandler(metadata, **db_params)

        export_options: Dict = utilities.read_config('export', 'csv_config.yaml')
        self.path = export_options.get('save_path', '')
        self.filename = export_options.get('filename', 'csv_export_{}'.format(datetime.now().__str__()))
        self.delimiter = export_options.get('seperation_sign', ',')

        query_options: Dict = utilities.read_config('query_options', 'csv_config.yaml')
        self.query_everything = query_options.get('query_everything', False)
        from_timestamp_str: str = query_options.get('from_timestamp', None)
        if from_timestamp_str:
            self.from_timestamp = dateparser.parse(from_timestamp_str, dayfirst=True)
        else:
            self.from_timestamp = datetime.min

        to_timestamp_str: str = query_options.get('to_timestamp', None)
        if to_timestamp_str is not None and to_timestamp_str.lower() != 'now':
            self.to_timestamp = dateparser.parse(to_timestamp_str, dayfirst=True)
        else:
            self.to_timestamp = datetime.utcnow()
        self.exchanges = query_options.get('exchanges', None)
        self.first_currencies = query_options.get('first_currencies', None)
        self.second_currencies = query_options.get('second_currencies', None)
        self.currency_pairs = query_options.get('currency_pairs', None)

        self.create_csv()

    def create_csv(self):
        ticker_data = self.database_handler.get_readable_tickers(self.query_everything,
                                                                 self.from_timestamp,
                                                                 self.to_timestamp,
                                                                 self.exchanges,
                                                                 self.currency_pairs,
                                                                 self.first_currencies,
                                                                 self.second_currencies)

        if not os.path.exists(self.path):
            os.makedirs(self.path)
        full_path: str = os.path.join(self.path, '{}.csv'.format(self.filename))
        print(full_path)
        with open(full_path, 'w', newline='') as file:
            writer = csv.writer(file, delimiter=self.delimiter)
            for tuple in ticker_data:
                writer.writerow(tuple)
