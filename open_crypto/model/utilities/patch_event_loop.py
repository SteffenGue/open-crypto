#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This class patches the open issue with nested asyncio EventLoops under several environments.
"""
import asyncio
import logging
from asyncio import AbstractEventLoop
from typing import Optional

import nest_asyncio


class PatchEventLoop:
    """
    Patch the nested-event-loop issue with several IDEs such as Jupyter-Notebook, Spyder, ect.
    """

    @staticmethod
    def _catch_event_loop() -> Optional[AbstractEventLoop]:
        """
        Catches an already running event-loop.

        @return: BaseEventLoop or None
        """
        return asyncio.get_event_loop()

    @staticmethod
    def check_event_loop_exists() -> bool:
        """
        Checks for an running event-loop.

        @return: bool.
        """
        return asyncio.get_event_loop().is_running()

    @staticmethod
    def apply_patch() -> None:
        """
        Prepares for nested event-loops or appends task to an already running event-loop.
        """
        logging.warning("You are attempting to start an nested asyncio EventLoop. \n"
                        "This is caused by design of several environments, such as Jupyter-Notebook or Spyder. \n"
                        "The current solution is the package 'nest_asyncio', which applies a patch. However, \n"
                        "if this package should depreciate in the future, extent this class by applying main.main() \n"
                        "as new task to the already running EventLoop.")
        nest_asyncio.apply()
