#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Module for data exporting into .csv or hdf5 format. The module is called from runner.py, reads-in a
configuration file and exports data into one of both mentioned formats.
"""

import inspect
import os
from datetime import datetime
from typing import Any

import pandas as pd
from dateutil import parser as date_parser
from sqlalchemy.orm.session import Session

from model.database import tables
from model.database.db_handler import DatabaseHandler
from model.database.tables import metadata
from model.utilities.time_helper import TimeHelper
from model.utilities.utilities import read_config, load_program_config


def database_session(filename: str = None, db_path: str = None) -> Session:
    """
    Returns an open SqlAlchemy-Session. The session is retrieved from the DatabaseHandler.
    @param filename: Name of the configuration file to init the DatabaseHandler
    @param db_path: Path to the database. Default: current working directory
    @return: SqlAlchemy-Session
    """
    min_return_tuples = load_program_config().get("request_settings").get("min_return_tuples", 1)
    db_handler = DatabaseHandler(metadata=metadata, path=db_path, min_return_tuples=min_return_tuples,
                                 **read_config(file=filename, section="database"))
    return db_handler.session_factory()


class CsvExport:
    """
    Class to actually query and save data. The file-format is given as input parameter, along with *args and
    **kwargs for the pd.to_csv(*args, **kwargs) and pd.to_hdf(*args, **kwargs).
    """

    def __init__(self, file: str = None):
        self.config = read_config(file=file, section=None)
        self.db_handler = DatabaseHandler(metadata, **self.config.get("database"))
        self.options = self.config["query_options"]
        self.filename = f"{self.options.get('table_name')}_{TimeHelper.now().strftime('%Y-%m-%dT%H-%M-%S')}"
        self.path = os.getcwd()

        # extract and convert starting point
        self.from_timestamp: str = self.options.get("from_timestamp", None)
        if self.from_timestamp:
            self.from_timestamp = date_parser.parse(self.from_timestamp, dayfirst=True)
        else:
            self.from_timestamp = datetime.min

        # extract and convert ending point
        self.to_timestamp: str = self.options.get("to_timestamp", None)
        if self.to_timestamp and self.to_timestamp != "now":
            self.to_timestamp = date_parser.parse(self.to_timestamp, dayfirst=True)
        else:
            self.to_timestamp = TimeHelper.now()

        # get table object to pass over as an argument in self.create_csv()
        table_names = dict()
        for name, obj in inspect.getmembers(tables):
            if inspect.isclass(obj):
                table_names.update({name: obj})
        self.table = table_names[self.options.get("table_name", None)]

    def load_data(self) -> pd.DataFrame:
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
        return pd.DataFrame(ticker_data)

    def export(self, data_type: str = "csv", *args: Any, **kwargs: Any) -> Any:
        """
        Exports the data in the specified format.
        @param data_type: String representation of the export format. Default: csv.
        """

        ticker_data = self.load_data()
        if self.filename.endswith(".csv"):
            output_path: str = os.path.join(self.path, self.filename)
        else:
            output_path: str = os.path.join(self.path, f"{self.filename}.csv")

        export_format = {"csv": {"function": ticker_data.to_csv,
                                 "parameters": ["path_or_buf", "sep", "decimal", "index"]},
                         "hdf": {"function": ticker_data.to_hdf,
                                 "parameters": ["path_or_buf"]}}

        parameters = {"path_or_buf": output_path,
                      "sep": self.options.get("delimiter", ";"),
                      "decimal": self.options.get("decimal", "."),
                      "index": False,
                      }

        parameters = {k: v for k, v in parameters.items() if k in export_format.get(data_type).get("parameters")}
        parameters.update(**kwargs)

        export_format.get(data_type, "csv").get("function")(*args, **parameters)
        print(output_path)
