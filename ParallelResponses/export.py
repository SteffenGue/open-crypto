from model.utilities.utilities import read_config
from model.database.tables import metadata
from model.database import tables
from model.database.db_handler import DatabaseHandler
from dateutil import parser as dateparser
from datetime import datetime
from typing import Dict
import pandas as pd
import inspect
import os


class CsvExport:

    def __init__(self,
                 file: str = None):

        self.config: Dict = read_config(file=file, section=None)
        self.db_handler = DatabaseHandler(metadata, **self.config['database'])
        self.options: Dict = self.config['query_options']
        self.filename = f"{self.options.get('table_name')}_{datetime.now().strftime('%Y-%m-%dT%H-%M-%S')}"
        self.path = os.getcwd()

        # extract and convert starting point
        self.from_timestamp: str = self.options.get('from_timestamp', None)
        self.from_timestamp: str = self.options.get('from_timestamp', None)
        if self.from_timestamp:
            self.from_timestamp = dateparser.parse(self.from_timestamp, dayfirst=True)
        else:
            self.from_timestamp = datetime.min

        # extract and convert ending point
        self.to_timestamp: str = self.options.get('to_timestamp', None)
        if self.to_timestamp is not None and self.to_timestamp != 'now':
            self.to_timestamp = dateparser.parse(self.to_timestamp, dayfirst=True)
        else:
            self.to_timestamp = datetime.utcnow()

        # get table object to pass over as an argument in self.create_csv()
        table_names = dict()
        for name, obj in inspect.getmembers(tables):
            if inspect.isclass(obj):
                table_names.update({name: obj})
        self.table = table_names[self.options.get('table_name', None)]

    def create_csv(self):
        """
        Receives from the DatabaseHandler the tuples that should be exported.
        The received tuples are based on the parameters set by the user in csv-config.yaml.
        The method tries to create the save-path if it not already exists.
        Creates or modifies the file.
        All previously stored content in the file will be erased.
        """
        ticker_data = self.db_handler.get_readable_query(self.table,
                                                         self.options.get('query_everything', None),
                                                         self.from_timestamp,
                                                         self.to_timestamp,
                                                         self.options.get('exchanges', None),
                                                         self.options.get('currency_pairs', None),
                                                         self.options.get('first_currencies', None),
                                                         self.options.get('second_currencies', None)
                                                         )
        ticker_data = pd.DataFrame(ticker_data)

        if self.filename.endswith('.csv'):
            full_path: str = os.path.join(self.path, self.filename)
        else:
            full_path: str = os.path.join(self.path, '{}.csv'.format(self.filename))
        print(full_path)
        ticker_data.to_csv(full_path, sep=self.options.get("delimiter", ","), index=False)


if __name__ == '__main__':
    CsvExport('csv_config').create_csv()