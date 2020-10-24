# -*- coding: utf-8 -*-

"""
Test module for testing api_scheduling.Mapping class

Authors:
    Carolina Keil,
    Martin Schorfmann

Since:
    27.09.2018

Version:
    08.11.2018
"""

import datetime
import unittest
from model.exchange.Mapping import Mapping


class TestMapping(unittest.TestCase):
    """Test class for Mapping."""

    # pylint: disable=too-many-public-methods

    def test_extract_value_split_index_zero(self):
        """Test of splitting a str and taking the index zero."""

        mapping = Mapping("exchange_name",
                          ["product_id"],
                          ["str", "split", "-", 0])
        result = mapping.extract_value({
            "product_id": "BTC-ETH"
        })
        self.assertEqual(result, "BTC")

    def test_extract_value_split_index_one(self):
        """Test of splitting a str and taking the index one."""

        mapping = Mapping("exchange_name",
                          ["product_id"],
                          ["str", "split", "-", 1])
        result = mapping.extract_value({
            "product_id": "BTC-ETH"
        })
        self.assertEqual(result, "ETH")

    def test_extract_value_split_index_two(self):
        """Test of splitting a str and taking the index two."""

        mapping = Mapping("exchange_name",
                          ["product_id"],
                          ["str", "split", "-", 2])
        result = mapping.extract_value({
            "product_id": "BTC-ETH-USD"
        })
        self.assertEqual(result, "USD")

    def test_extract_value_slice_first_half(self):
        """Test of slicing a str and taking the first half."""

        mapping = Mapping("exchange_name",
                          ["product_id"],
                          ["str", "slice", 0, 3])
        result = mapping.extract_value({
            "product_id": "BTCETH"
        })
        self.assertEqual(result, "BTC")

    def test_extract_value_slice_second_half(self):
        """Test of slicing a str and taking the second half."""

        mapping = Mapping("exchange_name",
                          ["product_id"],
                          ["str", "slice", 3, 6])
        result = mapping.extract_value({
            "product_id": "BTCETH"
        })
        self.assertEqual(result, "ETH")

    # def test_extract_value_bool_to_str_true(self):
    #     """Test of conversion from bool to str in case of True."""
    #
    #     mapping = Mapping("active",
    #                       ["active"],
    #                       ["bool", "str"])
    #     result = mapping.extract_value({
    #         "active": True
    #     })
    #     self.assertIsInstance(result, str)
    #     self.assertEqual(result, "True")
    #
    # def test_extract_value_bool_to_str_false(self):
    #     """Test of conversion from bool to str in case of False."""
    #
    #     mapping = Mapping("active",
    #                       ["active"],
    #                       ["bool", "str"])
    #     result = mapping.extract_value({
    #         "active": False
    #     })
    #     self.assertIsInstance(result, str)
    #     self.assertEqual(result, "False")

    def test_extract_value_str_to_bool_true(self):
        """Test of conversion from str to bool in case of True."""

        mapping = Mapping("active",
                          ["active"],
                          ["str", "bool"])

        result = mapping.extract_value({
            "active": "True"
        })

        self.assertIsInstance(result, bool)
        self.assertEqual(result, True)

    def test_extract_value_str_to_bool_true_lowercase(self):
        """Test of conversion from str to bool in case of lowercase True."""

        mapping = Mapping("active",
                          ["active"],
                          ["str", "bool"])

        result = mapping.extract_value({
            "active": "true"
        })

        self.assertIsInstance(result, bool)
        self.assertEqual(result, True)

    def test_extract_value_str_to_bool_true_uppercase(self):
        """Test of conversion from str to bool in case of uppercase True."""

        mapping = Mapping("active",
                          ["active"],
                          ["str", "bool"])

        result = mapping.extract_value({
            "active": "TRUE"
        })

        self.assertIsInstance(result, bool)
        self.assertEqual(result, True)

    def test_extract_value_str_to_bool_false(self):#
        """Test of conversion from str to bool in case of False."""

        mapping = Mapping("active",
                          ["active"],
                          ["str", "bool"])

        result = mapping.extract_value({
            "active": "False"
        })

        self.assertIsInstance(result, bool)
        self.assertEqual(result, False)

    def test_extract_value_str_to_bool_false_lowercase(self):
        """Test of conversion from str to bool in case of lowercase False."""

        mapping = Mapping("active",
                          ["active"],
                          ["str", "bool"])

        result = mapping.extract_value({
            "active": "false"
        })

        self.assertIsInstance(result, bool)
        self.assertEqual(result, False)

    def test_extract_value_str_to_bool_false_uppercase(self):
        """Test of conversion from str to bool in case of uppercase False."""

        mapping = Mapping("active",
                          ["active"],
                          ["str", "bool"])

        result = mapping.extract_value({
            "active": "FALSE"
        })

        self.assertIsInstance(result, bool)
        self.assertEqual(result, False)

    def test_extract_value_str_to_bool_false_anything(self):
        """Test of conversion from str to bool in case of anything."""

        mapping = Mapping("active",
                          ["active"],
                          ["str", "bool"])

        result = mapping.extract_value({
            "active": "anything"
        })

        self.assertIsInstance(result, bool)
        self.assertEqual(result, False)

    def test_extract_value_bool_to_int_true(self):
        """Test of conversion from bool to int in case of True."""

        mapping = Mapping("active",
                          ["active"],
                          ["bool", "int"])

        result = mapping.extract_value({
            "active": True
        })

        self.assertIsInstance(result, int)
        self.assertEqual(result, 1)

    def test_extract_value_bool_to_int_false(self):
        """Test of conversion from bool to int in case of False."""

        mapping = Mapping("active",
                          ["active"],
                          ["bool", "int"])

        result = mapping.extract_value({
            "active": False
        })

        self.assertIsInstance(result, int)
        self.assertEqual(result, 0)

    def test_extract_value_int_to_bool_one(self):
        """Test of conversion from int to bool in case of one."""

        mapping = Mapping("active",
                          ["active"],
                          ["int", "bool"])

        result = mapping.extract_value({
            "active": 1
        })

        self.assertIsInstance(result, bool)
        self.assertEqual(result, True)

    def test_extract_value_int_to_bool_zero(self):
        """Test of conversion from int to bool in case of zero."""

        mapping = Mapping("active",
                          ["active"],
                          ["int", "bool"])

        result = mapping.extract_value({
            "active": 0
        })

        self.assertIsInstance(result, bool)
        self.assertEqual(result, False)

    def test_extract_value_int_to_bool_two(self):
        """Test of conversion from int to bool in case of two."""

        mapping = Mapping("active",
                          ["active"],
                          ["int", "bool"])

        result = mapping.extract_value({
            "active": 2
        })

        self.assertIsInstance(result, bool)
        self.assertEqual(result, True)

    def test_extract_value_int_to_bool_neg_two(self):
        """Test of conversion from int to bool in case of negative two."""

        mapping = Mapping("active",
                          ["active"],
                          ["int", "bool"])

        result = mapping.extract_value({
            "active": -2
        })

        self.assertIsInstance(result, bool)
        self.assertEqual(result, True)

    def test_extract_value_int_fromtimestamp(self):
        """Test of conversion from an int timestamp to datetime."""

        mapping = Mapping("time",
                          ["time"],
                          ["int", "fromtimestamp"])

        result = mapping.extract_value({
            "time": 1538122622
        })

        self.assertIsInstance(result, datetime.datetime)
        # Different results depending on timezone
        # self.assertEqual(result, datetime.datetime(2018, 9, 28, 8, 17, 2))

    def test_extract_value_int_utcfromtimestamp(self):
        """Test of conversion from an int UTC timestamp to datetime."""

        mapping = Mapping("time",
                          ["time"],
                          ["int", "utcfromtimestamp"])

        result = mapping.extract_value({
            "time": 1538122622
        })

        self.assertIsInstance(result, datetime.datetime)

    def test_extract_value_int_fromtimestampms(self):
        """Test of conversion from an int timestamp with ms to datetime."""

        mapping = Mapping("time",
                          ["time"],
                          ["int", "fromtimestampms"])

        result = mapping.extract_value({
            "time": 1538122622123
        })

        self.assertIsInstance(result, datetime.datetime)
        self.assertEqual(result.microsecond, 123000)

    def test_extract_value_float_fromtimestamp(self):
        """Test of conversion from a float timestamp to datetime."""

        mapping = Mapping("time",
                          ["time"],
                          ["float", "fromtimestamp"])

        result = mapping.extract_value({
            "time": 1538122622.123
        })

        self.assertIsInstance(result, datetime.datetime)
        self.assertEqual(result.microsecond, 123000)

    def test_extract_value_float_utcfromtimestamp(self):
        """Test of conversion from a float UTC timestamp to datetime."""

        mapping = Mapping("time",
                          ["time"],
                          ["float", "utcfromtimestamp"])

        result = mapping.extract_value({
            "time": 1538122622.123
        })

        self.assertIsInstance(result, datetime.datetime)
        self.assertEqual(result.microsecond, 123000)

    def test_extract_value_str_to_int_zero(self):
        """Test of conversion from str to int in case of zero."""

        mapping = Mapping("number",
                          ["number"],
                          ["str", "int"])

        result = mapping.extract_value({
            "number": "0"
        })

        self.assertIsInstance(result, int)
        self.assertEqual(result, 0)

    def test_extract_value_str_to_int_one(self):
        """Test of conversion from str to int in case of one."""

        mapping = Mapping("number",
                          ["number"],
                          ["str", "int"])

        result = mapping.extract_value({
            "number": "1"
        })

        self.assertIsInstance(result, int)
        self.assertEqual(result, 1)

    def test_extract_value_str_to_int_two(self):
        """Test of conversion from str to int in case of two."""

        mapping = Mapping("number",
                          ["number"],
                          ["str", "int"])

        result = mapping.extract_value({
            "number": "2"
        })

        self.assertIsInstance(result, int)
        self.assertEqual(result, 2)

    def test_extract_value_str_to_int_twelve(self):
        """Test of conversion from str to int in case of twelve."""

        mapping = Mapping("number",
                          ["number"],
                          ["str", "int"])

        result = mapping.extract_value({
            "number": "12"
        })

        self.assertIsInstance(result, int)
        self.assertEqual(result, 12)

    def test_extract_value_str_to_int_neg_one(self):
        """Test of conversion from str to int in case negative one."""

        mapping = Mapping("number",
                          ["number"],
                          ["str", "int"])

        result = mapping.extract_value({
            "number": "-1"
        })

        self.assertIsInstance(result, int)
        self.assertEqual(result, -1)

    def test_extract_value_str_to_float_zero(self):
        """Test of conversion from str to float in case of zero."""

        mapping = Mapping("number",
                          ["number"],
                          ["str", "float"])

        result = mapping.extract_value({
            "number": "0.0"
        })

        self.assertIsInstance(result, float)
        self.assertEqual(result, 0.0)

    def test_extract_value_str_to_float_one(self):
        """Test of conversion from str to float in case of one."""

        mapping = Mapping("number",
                          ["number"],
                          ["str", "float"])

        result = mapping.extract_value({
            "number": "1.0"
        })

        self.assertIsInstance(result, float)
        self.assertEqual(result, 1.0)

    def test_extract_value_str_to_float_two(self):
        """Test of conversion from str to float in case of two."""

        mapping = Mapping("number",
                          ["number"],
                          ["str", "float"])

        result = mapping.extract_value({
            "number": "2.0"
        })

        self.assertIsInstance(result, float)
        self.assertEqual(result, 2.0)

    def test_extract_value_str_to_float_twelve(self):
        """Test of conversion from str to float in case of twelve."""

        mapping = Mapping("number",
                          ["number"],
                          ["str", "float"])

        result = mapping.extract_value({
            "number": "12.0"
        })

        self.assertIsInstance(result, float)
        self.assertEqual(result, 12.0)

    def test_extract_value_str_to_float_pi(self):
        """Test of conversion from str to float in case of Pi."""

        mapping = Mapping("number",
                          ["number"],
                          ["str", "float"])

        result = mapping.extract_value({
            "number": "3.141592654"
        })

        self.assertIsInstance(result, float)
        self.assertEqual(result, 3.141592654)

    def test_extract_value_str_to_float_neg_one(self):
        """Test of conversion from str to float in case of negative one."""

        mapping = Mapping("number",
                          ["number"],
                          ["str", "float"])

        result = mapping.extract_value({
            "number": "-1.0"
        })

        self.assertIsInstance(result, float)
        self.assertEqual(result, -1.0)

    def test_extract_value_str_to_float_neg_pi(self):
        """Test of conversion from str to float in case of negative Pi."""

        mapping = Mapping("number",
                          ["number"],
                          ["str", "float"])

        result = mapping.extract_value({
            "number": "-3.141592654"
        })

        self.assertIsInstance(result, float)
        self.assertEqual(result, -3.141592654)

    def test_extract_value_datetime_totimestamp(self):
        """Test of conversion from datetome to timestamp."""

        mapping = Mapping("date",
                          ["date"],
                          ["datetime", "totimestamp"])

        result = mapping.extract_value({
            "date": datetime.datetime(2018, 10, 11, 11, 20)
        })

        self.assertIsInstance(result, int)
        # Different results depending on timezone
        # self.assertEqual(result, 1539249600)

    def test_extract_value_datetime_totimestampms(self):
        """Test of conversion from datetome to timestamp with ms."""

        mapping = Mapping("date",
                          ["date"],
                          ["datetime", "totimestampms"])

        result = mapping.extract_value({
            "date": datetime.datetime(2018, 10, 11, 11, 20, 0, 123000)
        })

        self.assertIsInstance(result, int)
        # Different results depending on timezone
        # self.assertEqual(result, 1539249600123)

    def test_extract_value_datetime_utctotimestamp(self):
        """Test of conversion from datetome to UTC timestamp."""

        mapping = Mapping("date",
                          ["date"],
                          ["datetime", "utctotimestamp"])

        result = mapping.extract_value({
            "date": datetime.datetime(2018, 10, 11, 11, 20, 0)
        })

        self.assertIsInstance(result, int)
        # Different results depending on timezone

    def test_extract_value_str_strptime_date(self):
        """Test of conversion timestring via strptime in case of a date."""

        mapping = Mapping("date",
                          ["date"],
                          ["str", "strptime", "%Y-%m-%d"])

        result = mapping.extract_value({
            "date": "2018-10-11"
        })

        self.assertIsInstance(result, datetime.datetime)
        self.assertEqual(result, datetime.datetime(2018, 10, 11))

    def test_extract_value_str_strptime_datetime(self):
        """Test of conversion timestring via strptime in case of a datetime."""

        mapping = Mapping("date",
                          ["date"],
                          ["str", "strptime", "%Y-%m-%d %H:%M"])

        result = mapping.extract_value({
            "date": "2018-10-11 12:06"
        })

        self.assertIsInstance(result, datetime.datetime)
        self.assertEqual(result, datetime.datetime(2018, 10, 11, 12, 6))

    def test_extract_value_datetime_strftime_date(self):
        """Test of conversion from datetime via strftime in case of a date."""

        mapping = Mapping("date",
                          ["date"],
                          ["datetime", "strftime", "%Y-%m-%d"])

        result = mapping.extract_value({
            "date": datetime.datetime(2018, 10, 11)
        })

        self.assertIsInstance(result, str)
        self.assertEqual(result, "2018-10-11")

    def test_extract_value_datetime_strftime_datetime(self):
        """Test of conversion from datetime via strftime in case of a dt."""

        mapping = Mapping("date",
                          ["date"],
                          ["datetime", "strftime", "%Y-%m-%d %H:%M"])

        result = mapping.extract_value({
            "date": datetime.datetime(2018, 10, 11, 12, 6)
        })

        self.assertIsInstance(result, str)
        self.assertEqual(result, "2018-10-11 12:06")

    def test_extract_value_dict_key(self):
        """Test of extract value for dict_keys without further processing."""

        mapping = Mapping("pair",
                          ["dict_key"],
                          ["str"])

        result = mapping.extract_value({
            "BTC_USD": "value",
            "ETH_EUR": "value"
        })

        self.assertIsInstance(result, list)
        self.assertEqual(result, ["BTC_USD", "ETH_EUR"])

    def test_extract_value_dict_key_split_index_zero(self):
        """Test of extract value for dict_keys with split and index 0."""

        mapping = Mapping("pair",
                          ["dict_key"],
                          ["str", "split", "_", 0])

        result = mapping.extract_value({
            "BTC_USD": "value",
            "ETH_EUR": "value"
        })

        self.assertIsInstance(result, list)
        self.assertEqual(result, ["BTC", "ETH"])

    def test_extract_value_dict_key_split_index_one(self):
        """Test of extract value for dict_keys with split and index 1."""

        mapping = Mapping("pair",
                          ["dict_key"],
                          ["str", "split", "_", 1])

        result = mapping.extract_value({
            "BTC_USD": "value",
            "ETH_EUR": "value"
        })

        self.assertIsInstance(result, list)
        self.assertEqual(result, ["USD", "EUR"])


if __name__ == "__main__":
    unittest.main()
