#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This module is a wrapper around the whole package. Its main function is to export all relevant configuration files
to the current working directory of the user, start the program, establish database connections and export data
into csv-files.
"""

import sys
import os
# # Append path
# sys.path.append(os.path.dirname(__file__))

import os
import shutil
from typing import Dict
import pandas as pd

import main
from export import CsvExport, database_session
from model.utilities.utilities import read_config, get_all_exchanges_and_methods, prepend_spaces_to_columns
from model.database.tables import *


def check_path(path: str):
    """
    Checks if all resources are in the current working directory. If not, calls the function update_maps()
    """
    destination = path + "/resources"
    if not os.path.exists(destination):
        update_maps(path)


def update_maps(directory: str):
    """
    Copies everything from the folder "resources" into the current working directory. If files already exist,
    the method will override them (i.e. first delete and then copy).
    @type directory: Current working directory
    """

    print(f"Copying resources to {directory} ...")
    source = os.path.dirname(os.path.realpath(__file__)) + "/resources"

    destination = directory + "/resources"
    for src_dir, dirs, files in os.walk(source):
        dst_dir = src_dir.replace(source, destination, 1)
        try:
            dirs.remove('templates')
            dirs.remove('__pycache__')
        except ValueError:
            pass

        if not os.path.exists(dst_dir):
            os.makedirs(dst_dir)
        for file_ in files:
            src_file = os.path.join(src_dir, file_)
            dst_file = os.path.join(dst_dir, file_)
            if os.path.exists(dst_file):
                # in case of the src and dst are the same file
                if os.path.samefile(src_file, dst_file):
                    continue
                os.remove(dst_file)
            if not src_file.endswith('.py'):
                shutil.copy(src_file, dst_dir)


def get_session(filename: str, db_path: str):
    """
    Returns an open SqlAlchemy-Session. The session is obtained from the DatabaseHandler via the module export.py.
    Furthermore this functions imports all database defining classes to work with.

    @param db_path: path to the database. Default: current working directory
    @param filename: Name of the configuration file to init the DatabaseHandler
    @return: SqlAlchemy-Session
    """
    return database_session(filename=filename, db_path=db_path)


def exchanges_and_methods(return_dataframe: bool = False) -> pd.DataFrame:
    """
    Lists all exchanges and methods.
    as_dataframe: Specify if results are returned as pd.DataFrame.
    @type return_dataframe: boolean
    @return: Print or return dataframe
    @rtype: None or pd.DataFrame
    """
    dataframe = pd.DataFrame.from_dict(get_all_exchanges_and_methods())
    pd.set_option('display.max_rows', 500)

    if return_dataframe:
        return dataframe.transpose()
    else:
        print(prepend_spaces_to_columns(dataframe.transpose(), 3))


def get_config(filename: str = None) -> Dict:
    """
    Returns the actual config-file.
    @param filename: name of the config file.
    @return: Returns the current config.
    """
    return read_config(file=filename)


def get_config_template(csv: bool = False):
    """
    Creates a copy of the config templates and puts it into the resources/configs folder.
    @param csv: boolean: If True, create an csv-export config. Else create a config for the runner.
    """
    if csv:
        filename = "csv_config_template.yaml"
    else:
        filename = "config_template.yaml"

    source = os.path.dirname(os.path.realpath(__file__)) + "/resources/templates"
    destination = os.getcwd() + "/resources/configs"

    if os.path.exists(os.path.join(destination, filename)):
        os.remove(os.path.join(destination, filename))

    shutil.copy(os.path.join(source, filename),
                os.path.join(destination, filename))
    print("Created new config template.")


def export(file: str = None, data_type: str = 'csv', *args, **kwargs):
    """
    Calls the imported module CsvExport and the respective method create_csv(). This will take a csv-export config as
    input and write data into a csv-file depending on the configuration.
    @param data_type:
    @param file: Name of the csv-export configuration file.
    """
    CsvExport(file).export(data_type=data_type, *args, **kwargs)


def run():
    """
    First checks if all necessary folder are available (i.e. config and yaml-maps) and starts the program.
    """
    configuration_file = None
    working_directory = os.getcwd()

    check_path(working_directory)
    main.run(configuration_file, working_directory)


if __name__ == "__main__":
    run()
