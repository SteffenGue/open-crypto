import os
import sys

"""
Script to generate a global config_file variable. The goal is that the user is able to
create multiple configs and choose one when starting the program.

Right now, config_file is only used in the utilities.read_config()
"""
config_file: str = None
config_path = os.path.dirname(os.path.realpath(__file__))

if not config_file:
    # input_str = input("Enter config file name: ")
    input_str = 'config'
    if input_str in ['quit', 'exit']:
        sys.exit(0)

    config_file = config_path + "/" + input_str
    if '.yaml' not in config_file:
        config_file = config_file + '.yaml'


def setter(filename: str = None):
    """
    This function resets the global variable "config_file" to some user input (either the parameter filename
    or the keyboard input).
    :param filename: str: the config filename.
    """
    if not filename:
        filename = input('Config file not found. Retry: ')

        if filename in ['quit', 'exit']:
            sys.exit(0)
    if '.yaml' not in filename:
        filename = filename + '.yaml'
    global config_file
    config_file = config_path + "/" + filename
