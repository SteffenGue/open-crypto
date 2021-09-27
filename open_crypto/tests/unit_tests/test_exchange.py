#!/usr/bin/env python
# -*- coding: utf-8 -*-
# type: ignore[no-untyped-def]
"""
Test module for testing exchange.Exchange class

Authors:
    Steffen Guenther

Since:
    07.07.2021

Version:
    07.07.2021
"""
import os
import oyaml as yaml
from model.exchange.exchange import Exchange
import _paths  # pylint: disable=unused-import

path = os.getcwd() + "/open_crypto/tests/unit_tests"


with open(path + "/test_file.yaml", "r", encoding="UTF-8") as file:
    test_file: dict = yaml.load(file, Loader=yaml.FullLoader)


class TestExchange:
    """Test class for Exchange."""
    # pylint: disable=too-many-public-methods

    Exchange = Exchange(test_file, None, timeout=10, interval="seconds")

    def test_increase_interval(self) -> None:
        """
        Test function to increase interval from "seconds" to "minutes".
        """
        index = 2
        self.Exchange.interval = self.Exchange.interval_strings[index]
        self.Exchange.increase_interval()
        assert self.Exchange.interval == self.Exchange.interval_strings[index+1]

    def test_increase_max_interval(self) -> None:
        """
        Test that function can not increase interval higher "days".
        """
        index = - 1
        self.Exchange.interval = self.Exchange.interval_strings[index]
        self.Exchange.increase_interval()
        assert self.Exchange.interval == self.Exchange.interval_strings[index]

    def test_decrease_interval(self) -> None:
        """
        Test function that decreases interval from "hours" to "minutes".
        """
        index = 2
        self.Exchange.interval = self.Exchange.interval_strings[index]
        self.Exchange.decrease_interval()
        assert self.Exchange.interval == self.Exchange.interval_strings[index - 1]

    def test_decrease_min_interval(self) -> None:
        """
        Test that function cannot decrease interval lower than "seconds".
        """
        index = 0
        self.Exchange.interval = self.Exchange.interval_strings[index]
        self.Exchange.decrease_interval()
        assert self.Exchange.interval == self.Exchange.interval_strings[index]

    # def test_format_request_url(self):
    #     """
    #     # ToDo
    #     """
    #     pass
    #
    # def test_extract_request_urls(self):
    #     """
    #     # ToDo
    #     """
    #     pass
    #
    # def test_apply_currency_pair_format(self):
    #     """
    #     # ToDo
    #     """
    #     pass
    #
    # def test_format_currency_pairs(self):
    #     """
    #     # ToDo
    #     """
    #     pass
    #
    # def test_format_data(self):
    #     """
    #     # ToDo
    #     """
    #     pass
    #
    # def test_request(self):
    #     """
    #     # ToDo
    #     """
    #     pass
