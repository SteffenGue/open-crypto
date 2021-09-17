#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Validators to validate the CSV export template.

The module checks for existence, types and values of necessary parameters.

Validators structure:

- CsvExportTemplateValidator
  - LoadFileValidator
  - LoadYamlValidator
  - ExportTemplateValidator
    - BlockValidator
    - BlockExportValidator
    - BlockDatabaseValidator
    - BlockQueryOptionsValidator
"""
import sys
from typing import Any, Text, Dict

from model.validating.api_map_validators import LoadFileValidator, LoadYamlValidator
from model.validating.base import Report, Validator, CompositeValidator
from model.validating.errors import KeyNotInDictError, WrongTypeError, WrongValueError
from model.validating.errors import WrongCurrencyPairFormatError


class CsvExportTemplateValidator(CompositeValidator):
    """
    Validator for an CSV export template.

    Attributes:
        - value: The file name or file path as Text.
    """
    value: Text

    def validate(self) -> bool:
        """
        Validates the value.

        Validates the value attribute while generating a validation Report.

        @return: Whether further Validators may continue validating.
        """
        load_file = LoadFileValidator(self.value)
        is_file_loaded = load_file.validate()
        self.append_report(load_file)

        if not is_file_loaded:
            return False

        load_yaml = LoadYamlValidator(load_file)
        is_yaml_loaded = load_yaml.validate()
        self.append_report(load_yaml)

        if not is_yaml_loaded:
            return False

        export_template = ExportTemplateValidator(load_yaml.get_result_value())
        can_continue = export_template.validate()
        try:
            for report in export_template.report.reports:
                self.append_report(report)
        except (TypeError, Exception):
            self.append_report(export_template)

        return can_continue

    def result(self) -> bool:
        """
        TODO: Fill out
        """
        if not self.report:
            self.validate()

        return True


class ExportTemplateValidator(CompositeValidator):
    """
    Validator for the contents of an CSV export template.
    """

    def __init__(self, value: Dict[Text, Any]):
        """
        Constructor of ExportTemplateValidator.

        @param value: The value to get validated.
        """
        super().__init__(
            value,
            BlockValidator(value),
            BlockExportValidator(value["export"]),
            BlockDatabaseValidator(value["database"]),
            BlockQueryOptionsValidator(value["query_options"]),
        )


class BlockValidator(Validator):
    """
    Validator to check if all sections and blocks are present.
    """
    block = ["export", "database", "query_options"]

    def validate(self) -> bool:
        try:
            for key in self.value:
                if key not in BlockValidator.block:
                    raise KeyNotInDictError(key, self.value)
        except KeyNotInDictError as error:
            self.report = Report(error)
            return False
        else:
            self.report = Report(f"Export template contains all blocks: {BlockValidator.block}")
            return True


class BlockExportValidator(Validator):
    """
    Validator to check if the export block is valid.
    """

    def validate(self) -> bool:
        """
        Validates the value.

        @return: Whether further Validators may continue validating.
        """
        try:
            if "delimiter" not in self.value:
                raise KeyNotInDictError("delimiter", self.value)

            if not isinstance(self.value["delimiter"], str):
                raise WrongTypeError(str, type(self.value["delimiter"]))

            if "decimal" not in self.value:
                raise KeyNotInDictError("decimal", self.value)

            if not isinstance(self.value["decimal"], str):
                raise WrongTypeError(str, type(self.value["decimal"]))

            if self.value["decimal"] not in [".", ",", " "]:
                raise WrongValueError([".", ",", " "], self.value["decimal"], "decimal")

        except (KeyNotInDictError, WrongTypeError, WrongValueError) as error:
            self.report = Report(error)
            return False
        else:
            self.report = Report("Export block is valid")
            return True


class BlockDatabaseValidator(Validator):
    """
    Validator to check if the database block is valid.
    """
    db_strings = {
        "sqlite": ["sqltype", "db_name"],
        "mariadb": ["sqltype", "user_name", "password", "host", "port", "db_name"],
        "mysql": ["sqltype", "user_name", "password", "host", "port", "db_name"],
        "postgres": ["sqltype", "user_name", "password", "host", "port", "db_name"],
    }

    def validate(self) -> bool:
        """
        Validates the value.

        @return: Whether further Validators may continue validating.
        """
        try:
            if "sqltype" not in self.value:
                raise KeyNotInDictError("sqltype", self.value)

            for key in BlockDatabaseValidator.db_strings.get(self.value.get("sqltype")):

                if key not in self.value:
                    raise KeyNotInDictError(key, self.value)

                if self.value.get(key) is None:
                    raise WrongTypeError(str, type(self.value.get(key)), key)

        except (KeyNotInDictError, WrongTypeError) as error:
            self.report = Report(error)
            return False
        else:
            self.report = Report("Database connection string is valid")
            return True


class BlockQueryOptionsValidator(Validator):
    """
    Validator to check if the query options block is valid.
    """

    def validate(self) -> bool:
        """
        Validates the value.

        @return: Whether further Validators may continue validating.
        """
        try:
            if "table_name" not in self.value:
                raise KeyNotInDictError("table_name", self.value)

            if not isinstance(self.value["table_name"], str):
                raise WrongTypeError(str, type(self.value["table_name"]))

            if self.value["table_name"] not in ["HistoricRate", "Ticker", "Trade", "OrderBook"]:
                raise WrongValueError(
                    ["HistoricRate", "Ticker", "Trade", "OrderBook"], self.value["table_name"], "table_name"
                )

            if "query_everything" not in self.value:
                raise KeyNotInDictError("query_everything", self.value)

            if not isinstance(self.value["query_everything"], bool):
                raise WrongTypeError(bool, type(self.value["query_everything"]))

            if "from_timestamp" not in self.value:
                raise KeyNotInDictError("from_timestamp", self.value)

            if not isinstance(self.value["from_timestamp"], (str, type(None))):
                raise WrongTypeError((str, type(None)), type(self.value["from_timestamp"]))

            if "to_timestamp" not in self.value:
                raise KeyNotInDictError("to_timestamp", self.value)

            if not isinstance(self.value["to_timestamp"], (str, type(None))):
                raise WrongTypeError((str, type(None)), type(self.value["to_timestamp"]))

            if "exchanges" not in self.value:
                raise KeyNotInDictError("exchanges", self.value)

            if not isinstance(self.value["exchanges"], (str, type(None))):
                raise WrongTypeError((str, type(None)), type(self.value["exchanges"]))

            if "currency_pairs" not in self.value:
                raise KeyNotInDictError("currency_pairs", self.value)

            if not isinstance(self.value["currency_pairs"], (str, type(None))):
                raise WrongTypeError((str, type(None)), type(self.value["currency_pairs"]))

            if pair_string := self.value["currency_pairs"]:
                if pair_string == "all" or pair_string.count("-") >= 0 and pair_string.count("-") == pair_string.count(
                        ",") + 1:
                    raise WrongCurrencyPairFormatError(["-", ","], pair_string, "currency_pairs")

            if "first_currencies" not in self.value:
                raise KeyNotInDictError("first_currencies", self.value)

            if not isinstance(self.value["first_currencies"], (str, type(None))):
                raise WrongTypeError((str, type(None)), type(self.value["first_currencies"]))

            if "second_currencies" not in self.value:
                raise KeyNotInDictError("second_currencies", self.value)

            if not isinstance(self.value["second_currencies"], (str, type(None))):
                raise WrongTypeError((str, type(None)), type(self.value["second_currencies"]))

        except (KeyNotInDictError, WrongTypeError, WrongValueError) as error:
            self.report = Report(error)
            return False
        else:
            self.report = Report("Export block is valid")
            return True


if __name__ == "__main__":
    IS_VALID = CsvExportTemplateValidator("open_crypto/resources/templates/csv_export_template.yaml").validate()
    sys.exit(int(not IS_VALID))
