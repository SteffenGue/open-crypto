#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Module that contains helper classes to manage dates and times.

Classes:
- TimeHelper: Used to create/convert timezone aware (UTC+0) dates and times.

Enums:
- TimeUnit: Used to indicate the unit of timestamps.
"""

from datetime import datetime, timezone
from enum import IntEnum

from dateutil.parser import parse


class TimeUnit(IntEnum):
    """
    An enumeration to indicate the unit of timestamps.
    """
    SECONDS = 0
    MILLISECONDS = 1
    MICROSECONDS = 2
    NANOSECONDS = 3


class TimeHelper:
    """
    A helper class to create/convert dates and times.

    It ensures that all dates and times are timezone aware (UTC+0).
    """

    @staticmethod
    def now() -> datetime:
        """
        Get the current datetime (UTC+0).

        @return: The current datetime (UTC+0).
        """
        return datetime.now(tz=timezone.utc)

    @staticmethod
    def from_string(representation: str) -> datetime:
        """
        Get a datetime (UTC+0) from a given representation.

        @param representation: The string that represents a datetime.
        @return: The datetime (UTC+0) of the given representation.
        """
        return parse(representation).replace(tzinfo=timezone.utc)

    @staticmethod
    def from_timestamp(timestamp: float, unit: TimeUnit = TimeUnit.SECONDS) -> datetime:
        """
        Get a datetime (UTC+0) from a given timestamp.

        @param timestamp: The timestamp whose datetime is to be obtained.
        @param unit: The time unit in which the timestamp is given.

        @return: The datetime (UTC+0) of the given timestamp.
        """
        timestamp_in_sec: float = timestamp / (1000 ** int(unit))
        return datetime.fromtimestamp(timestamp_in_sec, tz=timezone.utc)

    @staticmethod
    def to_timestamp(date_time: datetime, unit: TimeUnit = TimeUnit.SECONDS) -> float:
        """
        Convert a datetime to a timestamp.

        @param date_time: The datetime to be converted.
        @param unit: The desired time unit of the timestamp.

        @return: The timestamp of the given datetime in the desired time unit.
        """
        return date_time.replace(tzinfo=timezone.utc).timestamp() * (1000 ** int(unit))
