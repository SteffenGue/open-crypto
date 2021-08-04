#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
TODO: Fill out module docstring.
Credit: https://stackoverflow.com/a/66558182
"""

from itertools import cycle
from shutil import get_terminal_size
from threading import Thread
from time import sleep


class Loader:
    def __init__(self, desc: str = "Loading...", end: str = "Done", timeout: float = 0.1):
        """
        A loader-like context manager

        @param desc (str, optional): The loader's description. Defaults to "Loading...".
        @param end (str, optional): Final print. Defaults to "Done!".
        @param timeout (float, optional): Sleep time between prints. Defaults to 0.1.
        """
        self.desc = desc
        self.end = end
        self.timeout = timeout

        self._thread = Thread(target=self._animate, daemon=True)
        self.steps = ["⢿", "⣻", "⣽", "⣾", "⣷", "⣯", "⣟", "⡿"]
        self.done = False

    def start(self):
        """
        # Todo
        """
        self._thread.start()
        return self

    def _animate(self):
        """
        # ToDo
        """
        for c in cycle(self.steps):
            if self.done:
                break
            print(f"\r{self.desc} {c}", flush=True, end="")
            sleep(self.timeout)

    def __enter__(self):
        """
        # ToDo
        """
        self.start()

    def stop(self):
        """
        # ToDo
        """
        self.done = True
        cols = get_terminal_size((80, 20)).columns
        print("\r" + " " * cols, end="", flush=True)
        print(f"\r{self.end}", flush=True)

    def __exit__(self, exc_type, exc_value, tb):
        """
        # ToDo
        """
        # handle exceptions with those variables
        self.stop()


