config_file: str = None
config_path = ""

if not config_file:
    config_file = config_path + input("Enter config file name: ")
    if '.yaml' not in config_file:
        config_file = config_file + '.yaml'


def setter(filename: str = None):
    if not filename:
        filename = input('Config file not found. Retry: ')
    if '.yaml' not in filename:
        filename = filename + '.yaml'
    global config_file
    config_file = config_path + filename
