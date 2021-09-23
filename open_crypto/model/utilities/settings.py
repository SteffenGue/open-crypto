#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This module contains possibilities to adjust several settings

Classes:
 - Setting: Contains advanced options for the program.
"""
import logging
import os
import shutil
from typing import Union, Any

import oyaml as yaml


class Settings:
    """
    Class to get and manipulate advanced program settings.
    """

    PATH = "/resources/configs/program_config/config.yaml"

    def __init__(self) -> None:
        self.config = Settings.get()
        self.copy = None

    @staticmethod
    def get() -> dict:
        """
        Returns the current program config.

        @return: The current program config.
        """
        with open(os.getcwd() + Settings.PATH, encoding="UTF-8") as file:
            return yaml.load(file, Loader=yaml.FullLoader)

    @staticmethod
    def _dump(config: dict) -> None:
        """
        Overwrites the current program config.

        @param config: The config to dump.
        """
        with open(os.getcwd() + Settings.PATH, "w", encoding="UTF-8") as file:
            yaml.dump(config, file)

    @staticmethod
    def set(block: str, key: str, val: Union[str, int, float]) -> None:
        """
        Sets a new value in the config file.

        @param block: Config file block
        @param key: Key within the block
        @param val: value to be set
        """
        try:
            with open(os.getcwd() + Settings.PATH, "r", encoding="UTF-8") as file:
                config = yaml.load(file, Loader=yaml.FullLoader)

            config.get(block).update({key: val})
            with open(os.getcwd() + Settings.PATH, "w", encoding="UTF-8") as file:
                yaml.dump(config, file)

        except (KeyError, FileNotFoundError):
            logging.error("Program config could not be updated.")
            print("Error updating program config!")

    @staticmethod
    def reset() -> None:
        """
        Creates a new template config file with all default settings.
        """
        filename = "program_config.yaml"

        source = os.path.dirname(os.path.realpath(__file__)) + "/resources/templates/"
        destination = os.getcwd() + "/resources/configs/program_config/"

        if os.path.exists(os.path.join(destination, filename)):
            os.remove(os.path.join(destination, filename.split("_")[1]))
        shutil.copy(os.path.join(source, filename),
                    os.path.join(destination, filename.split("_")[1]))

        print("Settings reset successful.")

    def _copy(self) -> None:
        """
        Copies the current config file.
        """
        self.copy = self.config.copy()

    def __enter__(self) -> object:
        """
        Enters the context manager.
        """
        self._copy()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """
        Exit the context manager.
        """
        self._dump(self.copy)
