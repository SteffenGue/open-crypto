#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This module contains possibilities to adjust several settings

Classes:
 - Setting: Contains advanced options for the program.
"""
from typing import Union, Any
import logging
import os
import shutil
import oyaml as yaml

import _paths


class Settings:
    """
    Class to get and manipulate advanced program settings.
    """

    PATH = _paths.all_paths.get("program_config_path")

    def __init__(self) -> None:
        self.config = Settings.get()
        self.copy = None

    @staticmethod
    def get() -> dict:
        """
        Returns the current program config.

        @return: The current program config.
        """
        with open(Settings.PATH, encoding="UTF-8") as file:
            return yaml.load(file, Loader=yaml.FullLoader)

    @staticmethod
    def _dump(config: dict) -> None:
        """
        Overwrites the current program config.

        @param config: The config to dump.
        """
        with open(Settings.PATH, "w", encoding="UTF-8") as file:
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
            with open(Settings.PATH, "r", encoding="UTF-8") as file:
                config = yaml.load(file, Loader=yaml.FullLoader)

            config.get(block).update({key: val})
            with open(Settings.PATH, "w", encoding="UTF-8") as file:
                yaml.dump(config, file)

        except (KeyError, FileNotFoundError):
            # Dump previous file if any unexpected error occurs.
            Settings._dump(Settings.config)
            logging.error("Program config could not be updated.")
            print("Error updating program config!")

    @staticmethod
    def reset() -> None:
        """
        Creates a new template config file with all default settings.
        """

        filename = "program_config.yaml"
        source = _paths.all_paths.get("template_path")
        destination = Settings.PATH

        if destination.exists():
            # Remove manipulated program config
            os.remove(destination)

        shutil.copy(source.joinpath(filename), destination)
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
