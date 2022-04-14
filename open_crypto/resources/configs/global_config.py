#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Singleton class providing a global configuration file.

Classes:
    - GlobalConfig
"""
from __future__ import annotations
import os
from typing import Optional

import _paths
# ToDo Alle paths hier hinterlegen


class GlobalConfig(object):
    """
    Global config built after the Singleton pattern. That is, there can only exists ONE instance of a class or,
    equivalently, all instances share the same state.

    The config-file will be set and read only from utilities.read_config().
    """
    __instance: Optional[GlobalConfig] = None
    __is_initialized = False

    def __new__(cls) -> GlobalConfig:
        """
        Create a new instance of __GlobalConfig if and only if none exists. Else returns the existing instance.
        That ensures that all instances of the class share the same state.
        """
        if cls.__instance is None:
            cls.__instance = object.__new__(cls)

        return cls.__instance

    def __init__(self) -> None:
        if not self.__is_initialized:
            self.__filename: Optional[str] = None
            self.__is_initialized = True

            # self.path = os.getcwd() + "/resources/configs/user_configs/"
            self.path = _paths.all_paths.get("user_config_path")
        try:
            # The first (os.getcwd()/...) is needed when the directory of the program and the resources differs.
            # That is the case for the Python Package installed via pip as we want the user to manipulate the resources
            # (i.e. config files and exchange mappings). The resources will be copied into the current working
            # directory and taken by the program from there.
            os.path.dirname(os.path.realpath(__file__)).index(self.path.__str__())
            self.path = os.path.dirname(os.path.realpath(__file__)) + "/user_configs/"
        except ValueError:
            pass

    def set_file(self, file: Optional[str] = None) -> None:
        """
        Sets self.__filename to either the variable given or an input string from the command prompt.
        The filename will be augmented with ".yaml" if needed.

        @param file: name of the config.
        @type file: str
        """
        if not file:
            file = input("Enter config file name [<filename> | quit/exit]: ").lower()
            if file in ["quit", "exit", "exit()", "quit()"]:
                raise SystemExit
                # sys.exit(0)
        if ".yaml" not in file:
            file = file + ".yaml"
        self.__filename = file

    @property  # TODO: Wrong use of property. Fix later with Steffen.
    def file(self, file: Optional[str] = None) -> str:
        """
        Returns the complete path to the config file. If the attribute self.__filename is not set yet,
        calls the method self.set_file.

        @param file: name of the config-file.
        @type file: str

        @return: complete path to config-file.
        @rtype: str
        """
        if not self.__filename:
            self.set_file(file=file)

        return "/".join([str(self.path), str(self.__filename)])
