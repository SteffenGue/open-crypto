#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Contains base validators and reports to represent validation results.

Classes:
    - Validator
    - CompositeValidator
    - ProcessingValidator
    - Report
    - CompositeReport
"""

from __future__ import annotations

import abc
import re
import textwrap
from typing import Text, Union, Any


class Validator(abc.ABC):
    """
    Base class for all validators.

    It offers functionality to validate a given value and to create a report of the validation process.

    Attributes:
        value:
            The value to get validated.
        report:
            The report created during validation.
    """

    def __init__(self, value: Union[Any, Validator]):
        """
        Constructor of Validator.

        @param value: The value to get validated or a validator having the result value.
        """
        if isinstance(value, Validator):
            self.value = value.get_result_value()
        else:
            self.value = value

        self.report = None

    def get_result_value(self) -> Any:
        """
        Gets the value of the validator.

        @return: The value of the Validator.
        """
        return self.value

    @abc.abstractmethod
    def validate(self) -> bool:
        """
        Validates the value and produces a report of the validation process.

        @return: True if the value is valid, False otherwise.
        """
        raise NotImplementedError

    def __bool__(self) -> bool:
        """
        @return: Returns the boolean value of its validation report.
        """
        return bool(self.report)


class CompositeValidator(Validator):
    """
    A composite validator that combines multiple validators.

    Attributes:
        validators:
            A List of children Validators.
    """

    def __init__(self, value: Union[Any, Validator], *child_validators: Validator):
        """
        Constructor of CompositeValidator.

        @param value: The value to get validated.
        @param child_validators: A variable-length sequence of children Validators.
        """
        super().__init__(value)

        self.validators = list(child_validators)
        self.report = CompositeReport()

    def append_validator(self, validator: Validator) -> None:
        """
        Adds a validator to the composite validator.

        @param validator: The validator to be added to the composite validator.
        """
        self.validators.append(validator)

    def append_report(self, report: Union[Validator, Report]) -> None:
        """
        Appends a Report or the Report of a Validator to the CompositeReport.

        @param report: A Validator whose Report will get appended or a Report.
        """
        assert isinstance(self.report, CompositeReport)
        self.report.append_report(report)

    def get_result_value(self) -> Any:
        """
        Returns the value or result.

        Returns its value if no special Validator is the last of its children.
        Otherwise the result value of the last Validator is returned.

        @return: The value or result value.
        """
        last_validator = self.validators[-1]

        if isinstance(last_validator, (ProcessingValidator, CompositeValidator)):
            return last_validator.get_result_value()

        return self.value

    def validate(self) -> bool:
        """
        Validates the value attribute while generating a validation Report.

        @return: Whether further Validators may continue validating.
        """
        assert isinstance(self.report, CompositeReport)

        for validator in self.validators:
            is_valid = validator.validate()
            self.report.append_report(validator.report)

            if not is_valid:
                return False

        return True


class ProcessingValidator(Validator):
    """
    Special Validator for processing a value.

    It produces a result value from the given value.

    Attributes:
        result:
            The result produced from the initial value.
    """

    def __init__(self, value: Any):
        """
        Constructor of ProcessingValidator.

        @param value: The value to get validated.
        """
        super().__init__(value)

        self.result = None

    @abc.abstractmethod
    def process(self) -> Any:
        """
        Processes the value.

        Returns the result value from processing the initial value.

        @return: The result value.
        """
        raise NotImplementedError

    def get_result_value(self) -> Any:
        """
        Gets the result value.

        @return: The result value.
        """
        return self.result

    def validate(self) -> bool:
        """
        Validates the value attribute while generating a validation Report.

        @return: Whether further Validators may continue validating.
        """
        self.result = self.process()
        return True


class _Valid:
    """
    Wrapper class for Exception or Text.

    Wrapper class for wrapping Exceptions or Text while providing a
    boolean value (False for Exceptions, True otherwise).

    Attributes:
        message:
            An Exception instance or a Text message (str).
    """

    def __init__(self, message: Union[Exception, Text]):
        """
        Construct a Valid object wrapping the specified message.

        @param message: An exception or text message to be wrapped.
        """
        self.message = message

    def __bool__(self) -> bool:
        """
        @return: False if a exception is wrapped, True otherwise.
        """
        return not isinstance(self.message, Exception)

    def __str__(self) -> Text:
        """
        A method for representing a message in text format.

        A text value returning "-" and the respective message, if message is a
        Exception, otherwise "+" and the respective message.

        @return: "-" for Exceptions, "+" otherwise.
        """
        sign = "+" if self else "-"
        return sign + " " + str(self.message)


class Report:
    """
    Report of Validation.

    Report of the Validation carried out in a Validator.
    Contains a list of Exceptions and Messages collected during Validation.

    Attributes:
        messages:
            List of wrapped Exceptions or Messages.
    """

    def __init__(self, *messages: Union[Exception, Text]):
        """
        Constructor of Report.

        @param messages: A variable-length sequence of Valid objects.
        """
        self.messages = [_Valid(message) for message in messages]

    def indented_report(self) -> Text:
        """
        Indents the generated report representation according to the
        opening and closing brackets.

        @return: The indented representation.
        """
        report = str(self)

        report = re.sub(r"\[", "[\n", report)
        report = re.sub(r",", ",\n", report)
        report = re.sub(r"]", "\n]", report)

        indented_lines = list()
        level = 0

        for line in report.splitlines():
            line = line.strip()

            if "]" in line:
                level -= 1

            indented_line = textwrap.indent(line, level * "  ")
            indented_lines.append(indented_line)

            if "[" in line:
                level += 1

        return "\n".join(indented_lines)

    def print_report(self) -> None:
        """
        Prints the Report's indented representation.
        """
        print(self.indented_report())

    def __bool__(self) -> bool:
        """
        A boolean value returning True if all elements in messages are True,
        otherwise returning False.

        @return: True if all messages are True, False otherwise.
        """
        return all(self.messages)

    def __str__(self) -> Text:
        """
        A method for representing a message in text format.

        A text value returning "-" and the respective message, if message is a
        Exception, otherwise "+" and the respective message.

        @return: "-" for Exceptions, "+" otherwise.
        """
        if len(self.messages) == 1:
            return str(self.messages[0])

        sign = "+" if self else "-"
        return sign + " " + str(self.messages)

    def __repr__(self) -> str:
        """
        A method for representing a message in text format.

        A text value returning "-" and the respective message, if message is a
        Exception, otherwise "+" and the respective message.

        @return: "-" for Exceptions, "+" otherwise.
        """
        return self.__str__()


class CompositeReport(Report):
    """
    CompositeReport consisting of multiple Reports.

    Attributes:
        reports:
            A list of child Reports.
    """

    def __init__(self, *reports: Report):
        """
        Constructor of CompositeReport.

        @param reports: A variable-length sequence of Report objects.
        """
        super().__init__()

        self.reports = list(reports)

    def append_report(self, report: Union[Report, Validator]) -> None:
        """
        Appends a Report or a Validator's Report to children reports.

        @param report: A Report or Validator containing a Report.
        """
        if isinstance(report, Validator):
            report = report.report

        self.reports.append(report)

    def __bool__(self) -> bool:
        """
        A boolean value returning True if all elements in reports are True,
        otherwise returning False.

        @return: True if all reports are True, False otherwise.
        """
        return all(self.reports)

    def __str__(self) -> Text:
        """
        A method for representing a report in text format.

        A text value returning "-" and the respective report, if report is a
        Exception, otherwise "+" and the respective report.

        @return: "-" for Exceptions, "+" otherwise.
        """
        sign = "+" if self else "-"
        return sign + " " + str(self.reports)

    def __len__(self) -> int:
        """
        Gets the report count.

        @return: The report count.
        """
        return len(self.reports)
