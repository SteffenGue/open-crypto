#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
TODO: Fill out module docstring.
"""

from itertools import cycle
from shutil import get_terminal_size
from threading import Thread
from time import sleep
from typing import Any, Union


class Loader:
    """
    # ToDo
    Credit to: https://stackoverflow.com/a/66558182
    """
    def __init__(self, desc: str = "Loading...", end: str = "Done", timeout: float = 0.1, max_counter: int = None):
        """
        A loader-like context manager

        @param desc (str, optional): The loader's description. Defaults to "Loading...".
        @param end (str, optional): Final print. Defaults to "Done!".
        @param timeout (float, optional): Sleep time between prints. Defaults to 0.1.
        """
        self.desc = desc
        self.end = end
        self.timeout = timeout
        self.counter = 0
        self.max_count = max_counter

        self._thread = Thread(target=self._animate, daemon=True)
        self.steps = ["⢿", "⣻", "⣽", "⣾", "⣷", "⣯", "⣟", "⡿"]
        self.done = False

    def start(self) -> object:
        """
        # Todo
        """
        self._thread.start()
        return self

    def increment(self, step_size: Union[int, float] = 1) -> None:
        """
        Increments the counter.
        """
        self.counter += step_size

    def _animate(self) -> None:
        """
        # ToDo
        """
        for step in cycle(self.steps):
            if self.done:
                break
            if self.max_count:
                progress = "{:.2f}".format((self.counter/self.max_count) * 100)
                print(f"\r{self.desc} {progress} % {step} ", flush=True, end="")
            else:
                print(f"\r{self.desc} {step}", flush=True, end="")
            sleep(self.timeout)

    def __enter__(self) -> None:
        """
        # ToDo
        """
        self.start()
        return self

    def stop(self) -> None:
        """
        # ToDo
        """
        self.done = True
        cols = get_terminal_size((80, 20)).columns
        print("\r" + " " * cols, end="", flush=True)
        print(f"\r{self.end}", flush=True)

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        """
        # ToDo
        """
        # handle exceptions with those variables
        self.stop()
