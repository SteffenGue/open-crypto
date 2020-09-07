import csv
import os
from datetime import datetime
from typing import Dict, List, Tuple

from model.database.db_handler import DatabaseHandler
import model.utilities.utilities as utilities
from model.database.tables import metadata
from dateutil import parser as dateparser


class CsvExporter:
    """
    Controls and guides the export of database-tuples as a csv-file.
    The actual parameters for the export can be set in csv_config.yaml.

    Attributes:
        database_handler: DatabaseHandler
            Instance of the handler for the database connection.
        path: str
            Path where the csv-file should be saved.
        name: str
            Name of the file.
        delimiter: str
            Delimeter which seperates the column-entries.

    Filter Attributes:
        query_everything: bool
            If everything that is stored in the database should be queried.
        from_timestamp: datetime
            Timestamp for the minimum time the request was sent.
        to_timestamp: datetime
            Timestamp for the maximum time the request was sent.
        exchanges: List[str]
            List of exchanges where tuples should be exported from.
        first_currencies: List[str]
            List of currencies that are the first one of a pair.
            Currency-pairs with first-currencies that can be found in this list will be exported.
        second_currencies: List[str]
            List of currencies that are the second one of a pair that should be exported.
            Currency-pairs with second-currencies that can be found in this list will be exported.
        currency_pairs: List[Tuple[str, str]]
            List of dictionaries of currency-pairs that should be exported.
            Currency-pairs which can be found in this list will be exported.
    """
    database_handler: DatabaseHandler
    path: str
    name: str
    delimiter: str
    query_everything: bool
    from_timestamp: datetime
    to_timestamp: datetime
    exchanges: List[str]
    first_currencies: List[str]
    second_currencies: List[str]
    currency_pairs: List[Tuple[str, str]]

    def __init__(self):
        """
        Creates a new CSV-Export process.
        The initializer takes in first the parameters described in csv_config.yaml
        and tries to export the data which is stored in the database
        based on the filters set by the user in the config-file.
        """
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
        """
        Receives from the DatabaseHandler the tuples that should be exported.
        The received tuples are based on the parameters set by the user in csv-config.yaml.
        The method tries to create the save-path if it not already exists.
        Creates or modifies the file.
        All previously stored content in the file will be erased.
        """
        ticker_data = self.database_handler.get_readable_tickers(self.query_everything,
                                                                 self.from_timestamp,
                                                                 self.to_timestamp,
                                                                 self.exchanges,
                                                                 self.currency_pairs,
                                                                 self.first_currencies,
                                                                 self.second_currencies)

        if not os.path.exists(self.path):
            os.makedirs(self.path)
        if self.filename.endswith('.csv'):
            full_path: str = os.path.join(self.path, self.filename)
        else:
            full_path: str = os.path.join(self.path, '{}.csv'.format(self.filename))
        print(full_path)
        with open(full_path, 'w', newline='') as file:
            writer = csv.writer(file, delimiter=self.delimiter)
            writer.writerows(ticker_data)


if __name__ == "__main__":
    CsvExporter()