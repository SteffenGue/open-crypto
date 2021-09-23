#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Class providing a loading bar animation. Either an instance of the class can be created or the class
may be used within the context manager framework.

Classes:
  - Loader
"""
import sys
from itertools import cycle
from shutil import get_terminal_size
from threading import Thread
from time import sleep
from typing import Any, Union

from colorama import Fore, Style, init

init()


class Loader:
    """
    Providing loading bar animation.

    Credit to: https://stackoverflow.com/a/66558182
    """

    def __init__(self, desc: str = "Loading...", end: str = "Done", timeout: float = 0.15, max_counter: int = None):
        """
        A loader-like context manager

        @param desc (str, optional): The loader's description. Defaults to "Loading...".
        @param end (str, optional): Final print. Defaults to "Done!".
        @param timeout (float, optional): Sleep time between prints. Defaults to 0.1.
        @param max_counter: Maximum value of the counter.
        """
        self.desc = desc
        self.end = end
        self.timeout = timeout
        self.counter = 0
        self.max_count = max_counter if isinstance(max_counter, (int, float)) and max_counter > 1 else None

        self._thread = Thread(target=self._animate, daemon=True)
        self.steps = ["|", "/", "-", "\\"]
        self.done = False

    def start(self) -> object:
        """
        Starts the loading bar.
        @return self.
        """
        self._thread.start()
        return self

    def increment(self, step_size: Union[int, float] = 1) -> None:
        """
        Increments the counter.
        @param step_size: Incrementation size.
        """
        self.counter += step_size

    def _animate(self) -> None:
        """
        Prints the loading bar
        """
        for step in cycle(self.steps):
            if self.done:
                break
            if self.max_count:
                progress = f"{(self.counter / self.max_count) * 100:.2f}"
                print(f"\r{self.desc} {progress} % {step} ", flush=True, end="")
            else:
                print(f"\r{self.desc} {step}", flush=True, end="")
            sleep(self.timeout)

    def stop(self, color: str = "green", in_place: bool = False) -> None:
        """
        print text to console, a wrapper to built-in print

        @param color: can be one of "red" or "green", or "default"
        @param in_place: whether to erase previous line and print in place
        """

        # this ANSI code lets us erase the current line
        erase_line = "\x1b[2K"
        color_name_to_code = {"default": "", "red": Fore.RED, "green": Style.BRIGHT + Fore.GREEN}

        self.done = True
        cols = get_terminal_size((80, 20)).columns
        print("\r" + " " * cols, end="", flush=True)
        sys.stdout.write(color_name_to_code[color] + f"\r{self.end}" + Style.RESET_ALL)
        if in_place:
            print("\r" + erase_line, end="")

        sys.stdout.flush()

    def __enter__(self) -> object:
        """
        Enter class as context manager.
        """
        self.start()
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        """
        Exits the context manager.
        """
        # handle exceptions with those variables
        self.stop()
