#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
TODO: Fill out module docstring.
"""

import os
import sys


class GlobalConfig(object):
    """
    Global config built after the Singleton pattern. That is, there can only exists ONE instance of a class or,
    equivalently, all instances share the same state.

    The config-file will be set and read only from utilities.read_config().
    """

    class __GlobalConfig:

        def __init__(self):
            self.__filename = None

            # The first is used normally. The second (os.getcwd()) is needed when the directory
            # of the program and the resources differs. That is the case for the Python Package as
            # we want the User to manipulate the resources (i.e. config files and exchange mappings). The resources
            # will be copied into the current working directory and taken by the program from there.
            self.path = os.path.dirname(os.path.realpath(__file__))
            # self.path = os.getcwd() + "/resources/configs/"

        def set_file(self, file: str = None):
            """
            Sets self.__filename to either the variable given or an input string from the command prompt.
            The filename will be augmented with ".yaml" if needed.

            @param file: name of the config.
            @type file: str
            """
            if not file:
                file = input("Enter config file name: ").lower()
                if file in ["quit", "exit"]:
                    sys.exit(0)
            if ".yaml" not in file:
                file = file + ".yaml"
            self.__filename = file

        @property
        def file(self, file: str = None) -> str:
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

    instance = None

    def __new__(cls):
        """
        Create a new instance of __GlobalConfig if and only if none exists. Else returns the existing instance.
        That ensures that all instances of the class share the same state.
        """
        if not GlobalConfig.instance:
            GlobalConfig.instance = GlobalConfig.__GlobalConfig()
        return GlobalConfig.instance
