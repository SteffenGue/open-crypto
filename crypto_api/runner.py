#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This module is a wrapper around the whole package. Its main function is to export all relevant configuration files
to the current working directory of the user, start the program, establish database connections and export data
into csv-files.
"""

import os
import shutil
from typing import Dict

import main
from export import CsvExport, database_session
from model.utilities.utilities import read_config

PATH = os.getcwd()


def check_path():
    """
    Checks if all resources are in the current working directory. If not, calls the function update_maps()
    """
    destination = PATH + "/resources"
    if not os.path.exists(destination):
        update_maps()


def update_maps(cwd: str = PATH):
    """
    Copies everything from the folder "resources" into the current working directory. If files already exist,
    the method will override them (i.e. first delete and then copy).
    @type cwd: Current working directory
    """

    print(f"Copying resources to {cwd}..")
    source = os.path.dirname(os.path.realpath(__file__)) + "/resources"

    destination = cwd + "/resources"
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


def get_path():
    """
    @return: Prints the path to the current working directory.
    """
    return os.getcwd()


def set_path():
    """
    Sets the path if it should differ from the current working directory.
    """
    global PATH
    PATH = input('New path: \n')
    print(f"Path set to {PATH}.")


def get_session(filename: str = None, db_path: str = PATH):
    """
    Returns an open SqlAlchemy-Session. The session is obtained from the DatabaseHandler via the module export.py.
    Furthermore this functions imports all database defining classes to work with.

    @param db_path: path to the database. Default: current working directory
    @param filename: Name of the configuration file to init the DatabaseHandler
    @return: SqlAlchemy-Session
    """
    return database_session(filename=filename, db_path=db_path)


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


def export(file: str = None):
    """
    Calls the imported module CsvExport and the respective method create_csv(). This will take a csv-export config as
    input and write data into a csv-file depending on the configuration.
    @param file: Name of the csv-export configuration file.
    """
    CsvExport(file).create_csv()


def run(cwd=PATH):
    """
    Firstly checks if all necessary folder are available (i.e. config and yaml-maps) and runs the program.
    @param cwd: The current working directory if not specified differently.
    """
    check_path()
    run(main.run(cwd))


if __name__ == '__main__':
    run(PATH)
