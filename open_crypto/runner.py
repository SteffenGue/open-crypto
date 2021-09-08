#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This module is a wrapper around the whole package. Its main function is to export all relevant configuration files
to the current working directory of the user, start the program, establish database connections and export data
into csv-files.
"""
import os
import shutil
from typing import Any, Optional, Dict
import pandas as pd
from sqlalchemy.orm.session import Session

# noinspection PyUnresolvedReferences
try:
    from open_crypto import _paths  # pylint: disable=unused-import
except (ImportError, Exception):
    import _paths  # pylint: disable=unused-import
import main
from model.utilities.kill_switch import KillSwitch
from model.utilities.export import CsvExport, database_session
from model.utilities.utilities import read_config, get_all_exchanges_and_methods, prepend_spaces_to_columns
from model.utilities.settings import Settings  # pylint: disable=unused-import
from model.utilities.github_downloader import GitDownloader
from model.database.tables import *  # pylint: disable=unused-import
from examples import Examples  # pylint: disable=unused-import


def update_maps() -> None:
    """
    Downloads the most recent exchange mappings from Github and saves them within the package directory.
    If the CWD differs from the package directory (i.e. the site-packages directory), further copys the updated
    resources to the current working directory.
    """

    GitDownloader.main()
    if os.getcwd() != _paths.package_path:
        copy_resources()

def check_path(path: str) -> None:
    """
    Checks if all resources are in the current working directory. If not, calls the function update_maps()

    @param path: The path.
    @type path: str
    """
    destination = path + "/resources"
    if not os.path.exists(destination):
        copy_resources(path)


def copy_resources(directory: str = os.getcwd()) -> None:
    """
    Copies everything from the folder "resources" into the current working directory. If files already exist,
    the method will override them (i.e. first delete and then copy).

    @param directory: The directory.
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


def get_session(filename: str, db_path: str = os.getcwd()) -> Session:
    """
    Returns an open SqlAlchemy-Session. The session is obtained from the DatabaseHandler via the module export.py.
    Furthermore this functions imports all database defining classes to work with.

    @param filename: Name of the configuration file to init the DatabaseHandler
    @type filename: str
    @param db_path: path to the database. Default: current working directory
    @type db_path: str

    @return: A database session.
    @rtype: Session
    """
    return database_session(filename=filename, db_path=db_path)


def exchanges_and_methods(return_dataframe: bool = False) -> Optional[pd.DataFrame]:
    """
    Lists all exchanges and methods.

    @param return_dataframe: If True, the dataframe is returned.
    @type return_dataframe: boolean

    @return: Print or return dataframe
    @rtype: Optional[pd.DataFrame]
    """
    working_directory = os.getcwd()
    check_path(working_directory)

    dataframe = pd.DataFrame.from_dict(get_all_exchanges_and_methods())
    pd.set_option('display.max_rows', 500)

    if return_dataframe:
        return dataframe.transpose()
    else:
        return prepend_spaces_to_columns(dataframe.transpose(), 3)


def get_config(filename: Optional[str] = None) -> Dict[str, Any]:
    """
    Parses the specified configuration file.

    @param filename: The name of the configuration file.
    @type filename: Optional[str]

    @return: Returns the parsed configuration.
    @rtype: dict[str, Any]
    """
    return read_config(file=filename)


def get_config_template(csv: bool = False) -> None:
    """
    Creates a copy of the config templates and puts it into the resources/configs folder.

    @param csv: If True, create an csv-export config. Else create a config for the runner.
    @type csv: bool
    """
    if csv:
        filename = "csv_export_template.yaml"
    else:
        filename = "request_template.yaml"

    source = os.path.dirname(os.path.realpath(__file__)) + "/resources/templates"
    destination = os.getcwd() + "/resources/configs/user_configs"

    if os.path.exists(os.path.join(destination, filename)):
        os.remove(os.path.join(destination, filename))

    shutil.copy(os.path.join(source, filename),
                os.path.join(destination, filename))
    print("Created new config template.")


def export(file: Optional[str] = None, file_format: str = "csv", *args: Any, **kwargs: Any) -> None:
    """
    Calls the imported module CsvExport and the respective method create_csv(). This will take a csv-export config as
    input and write data into a csv-file depending on the configuration.

    @param file: The name of the export file.
    @type file: Optional[str]
    @param file_format: The file format of the export file.
    @type file_format: str
    """
    CsvExport(file).export(data_type=file_format, *args, **kwargs)


def run(configuration_file: Optional[str] = None, kill_after: int = None) -> None:
    """
   Starts the program after checking if all necessary folder are available (i.e. config and yaml-maps).

    @param configuration_file: The configuration file.
    @type configuration_file: Optional[str]
    @param kill_after: Kills the thread.
    @type kill_after: int
    """
    working_directory = os.getcwd()
    check_path(working_directory)

    if kill_after and isinstance(kill_after, int):
        with KillSwitch() as switch:
            switch.set_timer(kill_after)
    try:
        main.run(configuration_file, working_directory)
    except SystemExit:
        pass

if __name__ == '__main__':
    update_maps()
