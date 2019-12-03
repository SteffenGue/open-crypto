import calendar
import datetime
import yaml
from configparser import ConfigParser


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
    ("none", "nowstrptime"):{
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

def read_config(section: str, filename='config.ini') -> dict:

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
    """

    with open('resources/running_exchanges/' + exchange + '.yaml', 'r') as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
        return data
