#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
TODO: Fill out module docstring.
"""

import inspect
import os
from datetime import datetime
from typing import Dict

import pandas as pd
from dateutil import parser as dateparser
from sqlalchemy import MetaData

from model.database import tables
from model.database.db_handler import DatabaseHandler
from model.database.tables import metadata
from model.utilities.time_helper import TimeHelper
from model.utilities.utilities import read_config


def database_session(filename: str = None, db_path: str = None, metadata: MetaData = metadata):
    """
    Returns an open SqlAlchemy-Session. The session is retrieved from the DatabaseHandler.
    @param metadata: Database metadata
    @param db_path: Path to the database. Default: current working directory
    @param filename: Name of the configuration file to init the DatabaseHandler
    @return: SqlAlchemy-Session
    """
    db_handler = DatabaseHandler(metadata=metadata, path=db_path, **read_config(file=filename, section="database"))
    return db_handler.sessionFactory()


class CsvExport:
    """
    TODO: Fill out
    """

    def __init__(self, file: str = None):
        self.config: Dict = read_config(file=file, section=None)
        self.db_handler = DatabaseHandler(metadata, **self.config["database"])
        self.options: Dict = self.config["query_options"]
        self.filename = f"{self.options.get('table_name')}_{TimeHelper.now().strftime('%Y-%m-%dT%H-%M-%S')}"
        self.path = os.getcwd()

        # extract and convert starting point
        self.from_timestamp: str = self.options.get("from_timestamp", None)
        if self.from_timestamp:
            self.from_timestamp = dateparser.parse(self.from_timestamp, dayfirst=True)
        else:
            self.from_timestamp = datetime.min

        # extract and convert ending point
        self.to_timestamp: str = self.options.get("to_timestamp", None)
        if self.to_timestamp and self.to_timestamp != "now":
            self.to_timestamp = dateparser.parse(self.to_timestamp, dayfirst=True)
        else:
            self.to_timestamp = TimeHelper.now()

        # get table object to pass over as an argument in self.create_csv()
        table_names = dict()
        for name, obj in inspect.getmembers(tables):
            if inspect.isclass(obj):
                table_names.update({name: obj})
        self.table = table_names[self.options.get("table_name", None)]

    def create_csv(self):
        """
        Receives from the DatabaseHandler the tuples that should be exported.
        The received tuples are based on the parameters set by the user in csv-config.yaml.
        The method tries to create the save-path if it not already exists.
        Creates or modifies the file.
        All previously stored content in the file will be erased.
        """
        ticker_data = self.db_handler.get_readable_query(self.table,
                                                         self.options.get("query_everything", None),
                                                         self.from_timestamp,
                                                         self.to_timestamp,
                                                         self.options.get("exchanges", None),
                                                         self.options.get("currency_pairs", None),
                                                         self.options.get("first_currencies", None),
                                                         self.options.get("second_currencies", None)
                                                         )
        ticker_data = pd.DataFrame(ticker_data)

        if self.filename.endswith(".csv"):
            output_path: str = os.path.join(self.path, self.filename)
        else:
            output_path: str = os.path.join(self.path, f"{self.filename}.csv")

        print(output_path)
        ticker_data.to_csv(output_path,
                           sep=self.options.get("delimiter", ";"),
                           decimal=self.options.get("decimal", "."),
                           index=False)
