import calendar
import datetime
import logging
import os
import pathlib
from datetime import timedelta
from pathlib import Path
from typing import List, Any, Dict

import dateutil.parser
import oyaml as yaml

from resources.configs.GlobalConfig import GlobalConfig

TYPE_CONVERSION = {
    ("bool", "int"): {
        "function": int,
        "params": 0
    },
    ('float', 'int'): {
        "function": int,
        "params": 0
    },
    ("int", "bool"): {
        "function": bool,
        "params": 0
    },
    ('int', 'div'): {
        'function': lambda integer, div: integer / (1 * div),
        'params': 1
    },
    ("int", "fromtimestamp"): {
        "function": datetime.datetime.utcfromtimestamp,
        "params": 0
    },
    ("int", "utcfromtimestamp"): {
        "function": lambda timestamp: datetime.datetime.utcfromtimestamp(
            int(timestamp)),
        "params": 0
    },
    ("int", "utcfromtimestampms"): {
        "function": lambda timestampms: datetime.datetime.utcfromtimestamp(
            int(timestampms) / 1000),
        "params": 0
    },
    ("int", "utcfromtimestampns"): {
        "function": lambda timestampms: datetime.datetime.utcfromtimestamp(
            int(timestampms) / 1000000),
        "params": 0
    },
    ("int", "fromtimestampms"): {
        "function": lambda timestampms: datetime.datetime.utcfromtimestamp(
            timestampms / 1000),
        "params": 0
    },
    ("int", "fromtimestampns"): {
        "function": lambda timestampns: datetime.datetime.utcfromtimestamp(
            timestampns / 1000000),
        "params": 0
    },
    ("int", "utcfromtimestamp-9"): {
        "function": lambda timestampns: datetime.datetime.utcfromtimestamp(
            timestampns / 1000000000),
        "params": 0
    },
    ("float", "fromtimestamp"): {
        "function": datetime.datetime.fromtimestamp,
        "params": 0
    },
    ("float", "utcfromtimestamp"): {
        "function": datetime.datetime.utcfromtimestamp,
        "params": 0
    },
    ("any", "value"): {
        "function": lambda number: True if float(number) > 0 else False,
        "params": 0
    },
    ("str", "bool"): {
        "function": lambda string: string.lower() == "true",
        "params": 0
    },
    ("str", "int"): {
        "function": int,
        "params": 0
    },
    ("str", "float"): {
        "function": float,
        "params": 0
    },
    ("str", "float_absolut"): {
        "function": lambda string: abs(float(string)),
        "params": 0
    },
    ("str", "floatNA"): {
        "function": lambda string: float(string) if string != "N/A" else None,
        "params": 0
    },
    ("str", "strptime"): {
        "function": lambda string, *args:
        datetime.datetime.strptime(string, args[0]),
        "params": 1
    },
    ("strptime_w_f", 'strptime_wo_f'): {
        'function': lambda string, *args: datetime.datetime.strptime(string.split(".")[0], *args),
        'params': 1
    },
    ("str", "split"): {
        "function": lambda string, *args: string.split(args[0])[args[1]] if args[0] in string else None,
        "params": 2
    },
    ("str", "splitupper"): {
        "function": lambda string, *args: string.split(args[0])[args[1]].upper(),
        "params": 2
    },
    ("str", "slice"): {
        "function": lambda string, *args: string[args[0]:args[1]],
        "params": 2
    },
    ("str", "upper"): {
        "function": lambda string: string.upper(),
        "params": 0
    },
    ("str", "lower"): {
        "function": lambda string: string.lower(),
        "params": 0
    },
    ('str', 'dateparser'): {
        'function': lambda string: dateutil.parser.parse(string),
        'params': 0
    },
    ("datetime", "strftime"): {
        "function": lambda time, *args: datetime.datetime.strftime(time, args[0]),
        "params": 1
    },
    ("dateparser", "totimestamp"): {
        "function": lambda time: int(time.timestamp()),
        "params": 0
    },
    ("datetime", "totimestamp"): {
        "function": lambda time: int(time.timestamp()),
        "params": 0
    },
    ("datetime", "totimestampms"): {
        "function": lambda time: int(round(time.timestamp() * 1000)),
        "params": 0
    },
    ("datetime", "utctotimestamp"): {
        "function": lambda time: calendar.timegm(time.utctimetuple()),
        "params": 0
    },
    ("strptime", "totimestamp"): {
        "function": lambda string, *args: int(datetime.datetime.timestamp(datetime.datetime.strptime(string, args[0]))),
        "params": 1
    },
    ("none", "nowstrptime"): {
        "function": lambda arg: datetime.datetime.strptime(datetime.datetime.now().strftime("%Y-%m-%d"), "%Y-%m-%d"),
        "params": 0
    },
    ("none", "now"): {
        "function": lambda: datetime.datetime.utcnow(),
        "params": 0
    },
    ("none", "now_format"): {
        "function": lambda arg: (datetime.datetime.utcnow()).__format__(arg),
        "params": 1
    },
    ("none", "constant"): {  # Returns the first argument
        "function": lambda *args: args[1],
        "params": 1
    },
    ('none', 'range'): {
        'function': lambda: range(1),
        'params': 0
    },
    ('value', 'map'): {
        # translate into buy/sell. Args: {0: 'buy', 1:'sell'} and arg[0] is the response value (i.e. 0/1)
        'function': lambda *args: {args[1]: args[2], args[3]: args[4]}[args[0]],
        'params': 4
    },
    ('str', 'split_at_del_or_index'): {
        'function': lambda string, *args: string.split(args[0])[args[2]] if len(string) != len(
            string.split(args[0])[0]) else string[:args[1]] if args[2] == 0 else string[args[1]:],
        'params': 3  # delimiter, index, 0 or 1 aka. left or right
    },
    ('none', 'now_timestamp'): {
        'function': lambda: int(datetime.datetime.timestamp(datetime.datetime.utcnow())),
        'params': 0
    },
    ('none', 'now_timestampms'): {
        'function': lambda: int(datetime.datetime.timestamp(datetime.datetime.utcnow()) * 1000),
        'params': 0
    },
    ('now', 'timedelta'): {
        'function': lambda delta: int(datetime.datetime.timestamp(datetime.datetime.utcnow() -
                                                                  timedelta(days=int(delta)))),
        'params': 1
    },
    ('datetime', 'timedelta'): {
        'function': lambda time, interval, delta: int(
            datetime.datetime.timestamp(time - timedelta(**{interval: int(delta)}))),
        'params': 2
    },
    ('utcfromtimestamp', 'timedelta'): {
        'function': lambda time, interval, value: datetime.datetime.utcfromtimestamp(time) - timedelta(
            **{interval: value}) if
        isinstance(time, int) else dateutil.parser.parse(time) - timedelta(**{interval: value}),
        'params': 2
    },
    ('datetime', 'timedeltams'): {
        'function': lambda time, interval, delta: int(
            datetime.datetime.timestamp(time - timedelta(**{interval: int(delta)}))) * 1000,
        'params': 2
    },
    ('datetime', 'timestamp'): {
        'function': lambda time: int(time.timestamp()),
        'params': 0
    },
    ('datetime', 'timestampms'): {
        'function': lambda time: int(time.timestamp()) * 1000,
        'params': 0
    },
    ("datetime", "format"): {
        "function": lambda time, arg: time.__format__(arg),
        "params": 1
    },
    ("timedelta", "fromtimestamp"): {
        "function": lambda time, arg: datetime.datetime.fromtimestamp(time).__format__(arg),
        "params": 1
    },
}
"""
    Type Conversions used to convert extracted values from the API-Response into the desired type ("first", "second").
    The values are specified in the .yaml-file of each exchange under the "mapping" of each method.
    The function is called in the Mapping Class of utilities.py under the method convert_types().

    "first":
        The actual type extracted from the API-Request (.json)
    "second":
        The desired type to convert
    "function":
        the actual function to apply
    "params":
        the number of additional parameters needed
"""


def read_config(file: str = None, section: str = None) -> Dict[str, Any]:
    """
    @param section: str
        Name of the section the information is stored in.
    @param file: str
        Name of the config file.
    @return: Dict
        Parameters for the program as a dictionary.
        Keys are the names of the parameters in the config-file.
    """
    if file:
        GlobalConfig().set_file(file)

    while True:
        try:
            filename = GlobalConfig().file
            config_yaml = open(filename)
            break
        except FileNotFoundError:
            print("File not found. Retry!")
            GlobalConfig().set_file()

    config_dict: Dict = yaml.load(config_yaml, Loader=yaml.FullLoader)
    config_yaml.close()

    if not section:
        return config_dict

    for general_section in config_dict.keys():
        if section == general_section:
            return config_dict[general_section]

        for nested_section in config_dict[general_section].keys():
            if section == nested_section:
                return config_dict[general_section][nested_section]

    raise Exception()


def yaml_loader(exchange: str):
    """
    Loads, reads and returns the data of a .yaml-file specified by the param exchange.

    @param exchange: str
        the file name to load (exchange)
    @return: data: dict
        returns a dict of the loaded data from the .yaml-file
    @exceptions Exception: the .yaml file could not be evaluated for a given exchange
    """
    path = read_config(file=None, section='utilities')['yaml_path']
    try:
        with open(path + exchange + '.yaml', 'r') as file:
            return yaml.load(file, Loader=yaml.FullLoader)
    except Exception as ex:
        print(f"Error loading yaml of {exchange}. Try validating the file or look in the log-files.")
        print(ex)
        logging.exception(f"Error loading yaml of {exchange}.\n", ex)
        raise ex


def get_exchange_names() -> List[str]:
    """
    Gives information about all exchange that the program will send
    requests to. This means if the name of a exchange is not part of the
    list that is returned, the program will not send any request to said
    exchange.
    @param: session: orm_session
        Connection to the Database in order to query all ACTIVE exchange.
    @return: List[str]
        Names from all the exchange, which have a .yaml-file in
        the directory described in YAML_PATH.
    """
    yaml_path = read_config(file=None, section='utilities')['yaml_path']
    path_to_resources: Path = pathlib.Path().parent.absolute()

    exchanges = os.listdir(Path.joinpath(path_to_resources, yaml_path))
    exchanges = [x[:-5] for x in exchanges if x.endswith(".yaml")]
    exchanges.sort()

    return exchanges
