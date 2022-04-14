#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Module provides a kill switch to safely terminate the data collector.

Classes:
  - KillSwitch: Singleton Class.
"""

from __future__ import annotations

import threading
from typing import Optional, Any


class KillSwitch(object):
    """
    Global kill-switch built after the Singleton pattern. That is, there can only exist ONE instance of a class or,
    equivalently, all instances share the same state.

    The KillSwitch terminates the data collector after writing the data into the database and before making
    new requests.
    """
    __instance: Optional[KillSwitch] = None
    __is_initialized = False

    def __new__(cls) -> KillSwitch:
        """
        Creates a new instance of the class, if and only if, none exists.
        """
        if cls.__instance is None:
            cls.__instance = object.__new__(cls)

        return cls.__instance

    def __init__(self) -> None:
        """
        Constructor
        """
        if not self.__is_initialized:
            self.__is_initialized = True
            self.stay_alive = True

    def kill(self) -> None:
        """
        Set state to False in order to kill process.
        """
        self.stay_alive = False

    def reset(self) -> None:
        """
        Reset the kill-switch.
        """
        self.stay_alive = True

    def set_timer(self, timer: int) -> None:
        """
        Sets the timer for the kill-switch.
        @param timer: Seconds to kill thread.
        """
        thread = threading.Timer(timer, self.kill)
        thread.start()

    def __enter__(self) -> object:
        """
        Enters the context manager.
        """
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> True:
        """
        Kill the process and exits the context manager.
        """
        self.reset()
        return True
