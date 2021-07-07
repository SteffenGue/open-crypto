#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
TODO: Fill out module docstring.
"""

import os
from typing import Optional

from tests.yaml_tests.api_map_validation import ApiMapFileValidator, Report

YAML_PATH = "../resources/running_exchanges/all/"


class Validate:
    """
    This class will validate a single exchange yaml-file. The first method ValidateMapFile checks the yaml-file
    if in the right format, i.e. performs unit-testing.
    The second methods ValidateYAML plugs the yaml-file in a cooked-down version of the main program and checks
    the functionality.
    """

    def __init__(self, exchange_name: str):
        self.exchange_name = exchange_name

    def validate(self) -> bool:
        """
        TODO: Fill out
        @return:
        """
        api_map = ApiMapFileValidator(YAML_PATH + self.exchange_name + ".yaml")
        api_map.validate()

        if bool(api_map.report):
            return True
        else:
            os.makedirs("reports/", exist_ok=True)
            with open("reports/report_" + self.exchange_name + ".txt", "w") as report:
                report.writelines(api_map.report.indented_report())

            print("API Map is Invalid! \n"
                  f"Inspect report: {'~/reports/report_' + self.exchange_name + '.txt'}")
            self.report_error(api_map.report)
            return False

    def report_error(self, report: Report) -> Optional[Report]:
        """
        Recursive method to find the lowest False in a nested list
        """
        if bool(report):
            return report
        else:
            for nested_report in report.reports:
                if not bool(nested_report):
                    try:
                        self.report_error(nested_report)
                    except AttributeError:
                        print(nested_report)
                        break


if __name__ == "__main__":
    exchange = input("Enter the exchange to validate (or 'all'): ")

    if exchange == "all":
        exchanges = [os.path.splitext(file)[0] for file in os.listdir(YAML_PATH) if file.endswith(".yaml")]

        valid_count = 0  # pylint: disable=C0103
        for exchange in exchanges:
            valid_count += Validate(exchange).validate()

        print(f"Valid Exchanges: {round(valid_count / len(exchanges) * 100, 2)} %")
    else:
        Validate(exchange).validate()
