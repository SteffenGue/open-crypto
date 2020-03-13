import calendar
import datetime
import os
from typing import List, Any, Dict, Set
from dictionary import ExceptionDict
import yaml
from configparser import ConfigParser
from db_handler import DatabaseHandler

TYPE_CONVERSION = {

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

    : {  # Tested
        "function": str,
        "params": 0
    },
    ("bool", "int"): {  # Tested
        "function": int,
        "params": 0
    },
    ("int", "bool"): {  # Tested
        "function": bool,
        "params": 0
    },
    ("int", "fromtimestamp"): {  # Partially tested
        "function": datetime.datetime.fromtimestamp,
        "params": 0
    },
    ("int", "utcfromtimestamp"): {  # Partially tested
        "function": datetime.datetime.utcfromtimestamp,
        "params": 0
    },
    ("int", "fromtimestampms"): {  # Partially tested
        "function": lambda timestampms: datetime.datetime.fromtimestamp(
            timestampms / 1000),
        "params": 0
    },
    ("int", "fromtimestampns"): {  # Not tested
        "function": lambda timestampns: datetime.datetime.fromtimestamp(
            timestampns / 1000000),
        "params": 0
    },
    ("float", "fromtimestamp"): {  # Partially tested
        "function": datetime.datetime.fromtimestamp,
        "params": 0
    },
    ("float", "utcfromtimestamp"): {  # Partially tested
        "function": datetime.datetime.utcfromtimestamp,
        "params": 0
    },
    ("str", "bool"): {  # Tested
        "function": lambda string: string.lower() == "true",
        "params": 0
    },
    ("str", "int"): {  # Tested
        "function": int,
        "params": 0
    },
    ("str", "float"): {  # Tested
        "function": float,
        "params": 0
    },
    ("str", "floatNA"): {
        "function": lambda string: float(string) if string != "N/A" else None,
        "params": 0
    },
    ("str", "strptime"): {  # Partially tested
        "function": lambda string, *args:
        datetime.datetime.strptime(string, args[0]),
        "params": 1
    },
    ("str", "split"): {  # Tested
        "function": lambda string, *args: string.split(args[0])[args[1]],
        "params": 2
    },
    ("str", "splitupper"): {
        "function": lambda string, *args: string.split(args[0])[args[1]].upper(),
        "params": 2
    },
    ("str", "slice"): {  # Partially tested
        "function": lambda string, *args: string[args[0]:args[1]],
        "params": 2
    },
    ("str", "upper"): {  # Not tested
        "function": lambda string: string.upper(),
        "params": 0
    },
    ("str", "lower"): {  # Not tested
        "function": lambda string: string.lower(),
        "params": 0
    },
    ("datetime", "strftime"): {  # Partially tested
        "function": lambda time, *args:
        datetime.datetime.strftime(time, args[0]),
        "params": 1
    },
    ("datetime", "totimestamp"): {  # Partially tested
        "function": lambda time: int(time.timestamp()),
        "params": 0
    },
    ("datetime", "totimestampms"): {  # Partially tested
        "function": lambda time: int(round(time.timestamp() * 1000)),
        "params": 0
    },
    ("datetime", "utctotimestamp"): {  # Partially tested
        "function": lambda time: calendar.timegm(time.utctimetuple()),
        "params": 0
    },
    ("none", "nowstrptime"): {
        "function": lambda arg: datetime.datetime.strptime(datetime.datetime.now().strftime("%Y-%m-%d"), "%Y-%m-%d"),
        "params": 0
    },
    ("none", "now"): {
        "function": lambda arg: datetime.datetime.now(),
        "params": 0
    },
    ("none", "constant"): {  # Returns the first argument
        "function": lambda arg, *args: args[0],
        "params": 1
    }
}


"""A dictionary containing lambda function calls in order to get request parameters variable. The function calls 
will be stored in the respective .yaml-file of each exchange and executed, outside the yaml environment, 
during the preparation of the API request.

'name' : call name of the lambda function
'function' : the actual lambda function to execute
'params' : amount of additional parameters if necessary.
'session' : ORM-Session if necessary.
"""
REQUEST_PARAMS = {
    "add": {
        "name": 'add',
        "function": lambda x: x+1,
        "params": 1,
        "session": False
    },
    "exchanges": {
        "name": 'exchanges',
        "function": lambda session, x: session.query(x),
        'params': 1,
        "session": True
    }
}


def read_config(section: str, filename='config.ini') -> Dict[str, Any]:
    """
    Reads the config.ini file specified in by the filename parameter

    :param section: str
        specifies the section to read from the config-file
    :param filename: str
        specifies the filename to read. Default: 'config.ini'
    :return: parameters: dict
        returns a dictionary of parameters
    """

    parser = ConfigParser()
    parser.read(filename)

    parameters = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            parameters[param[0]] = param[1]
    else:
        raise Exception('Section {} not found in file {}'.format(section, filename))

    return parameters


def yaml_loader(exchange: str):
    """
    Loads, reads and returns the data of a .yaml-file specified by the param exchange.

    :param exchange: str
        the file name to load (exchange)
    :return: data: dict
        returns a dict of the loaded data from the .yaml-file
    :exceptions Exception: the .yaml file could not be evaluated for a given exchange
    """

    with open(YAML_PATH + exchange + '.yaml', 'r') as f:
        try:
            data = yaml.load(f, Loader=yaml.FullLoader)
            return data
        except Exception as ex:
            # create an instance of the exception dictionary to safe the exchange which have thrown the exchange
            exception = ExceptionDict()
            exception.get_dict()['{}'.format(exchange)] = 1

def get_exchange_names(get_inactive_exchanges) -> Set[str]:
    """
    Gives information about all exchanges that the program will send
    requests to. This means if the name of a exchange is not part of the
    list that is returned, the program will not send any request to said
    exchange.
    :param: get_inactive_exchanges: DatabaseHandler method
        DatabaseHandler method to query all inactive exchanges from the database.
    :return: List[str]
        Names from all the exchanges, which have a .yaml-file in
        the directory described in YAML_PATH.
    """

    query = get_inactive_exchanges()

    inactive_exchanges = set([exchange for exchange, in query])
    exchanges_list = os.listdir(YAML_PATH)
    exchange_names = set([str(x.split(".")[0]) for x in exchanges_list if ".yaml" in x])

    exchanges = exchange_names - inactive_exchanges
    # exchange_names.sort()
    return exchanges

#Constant that contains the path to the yaml-files of working exchanges.
YAML_PATH = read_config('utilities')['yaml_path']
