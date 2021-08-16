#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
A command line tool to validate the exchange YAMLs.

How to use:
python validate.py { all | <exchange_name> }
"""

import os
import sys
from typing import Optional

from model.validating.base import Report, CompositeReport
from model.validating.validators import ApiMapFileValidator

YAML_PATH = "resources/running_exchanges/all/"


class ExchangeValidator:
    """
    A validator that validates the YAML of a single exchange.
    """

    def __init__(self, exchange_name: str):
        """
        Create a new ExchangeValidator instance.

        @param exchange_name: The name of the exchange to be validated.
        """
        self.exchange_name = exchange_name

    def validate(self) -> bool:
        """
        Validate the exchange's YAML.

        @return: True if the YAML of the exchange is valid, False otherwise.
        """
        validator = ApiMapFileValidator(f"{YAML_PATH}/{self.exchange_name}.yaml")
        validator.validate()

        if validator.report:
            return True

        os.makedirs("reports/", exist_ok=True)
        with open("reports/report_" + self.exchange_name + ".txt", "w") as report:
            report.writelines(validator.report.indented_report())

        print("API Map is Invalid! \n"
              f"Inspect report: {'~/reports/report_' + self.exchange_name + '.txt'}")
        self.report_error(validator.report)
        return False

    def report_error(self, report: Report) -> Optional[Report]:
        """
        Recursive method to find the lowest False in a nested list
        """
        if report:
            return report

        if not isinstance(report, CompositeReport):
            return None

        for nested_report in report.reports:
            if not nested_report:
                try:
                    self.report_error(nested_report)
                except AttributeError:
                    print(nested_report)
                    break


def validate_exchange(exchange_name: str) -> bool:
    """
    Validate the YAML of the specified exchange and print a human readable result.

    @param exchange_name: The exchange whose YAML is to be validated.

    @return: True if the YAML of the exchange is valid, False otherwise.
    """
    is_valid = ExchangeValidator(exchange_name).validate()
    print(f"Exchange: {exchange_name}, Valid: {is_valid}")
    return is_valid


if __name__ == "__main__":

    if len(sys.argv) == 1:
        print("How to use:")
        print("python validate.py { all | <exchange_name> }")
        sys.exit(1)

    exchange = sys.argv[1]

    if exchange != "all":
        sys.exit(int(not validate_exchange(exchange)))
    else:
        exchanges = [os.path.splitext(file)[0] for file in os.listdir(YAML_PATH) if file.endswith(".yaml")]

        valid_count = 0  # pylint: disable=C0103
        for exchange in exchanges:
            valid_count += int(validate_exchange(exchange))

        print(f"Valid Exchanges: {valid_count}/{len(exchanges)} ({int(valid_count / len(exchanges) * 100)} %)")
        sys.exit(int(valid_count != len(exchanges)))
