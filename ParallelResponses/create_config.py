import oyaml as yaml
from typing import Dict, Any
import sys


def read_config(filename='config.yaml') -> Dict[str, Any]:
    """
    @param section: str
        Name of the section the information is stored in.
    @param filename: str
        Name of the config file.
    @return:
        Parameters for the program as a dictionary.
        Keys are the names of the parameters in the config-file.
    """
    template = "resources/templates/config_template.yaml"
    try:
        config_yaml = open(filename)
        config_dict: Dict = yaml.load(config_yaml, Loader=yaml.FullLoader)

        if 'jobs' not in config_dict.keys():
            print('Trouble loading the config file. Switching to the config_template.')
            config_yaml = open(template)
            config_dict: Dict = yaml.load(config_yaml, Loader=yaml.FullLoader)

    except Exception as e:
        print('Trouble loading the config file. Switching to the config_template.')
        config_yaml = open(template)
        config_dict: Dict = yaml.load(config_yaml, Loader=yaml.FullLoader)
    finally:
        return config_dict


def write_config(new_config: dict, filename='config.yaml'):
    with open("/resources/configs/" + filename, 'w') as config_yaml:
        yaml.dump(new_config, config_yaml)


template: dict = {
    'yaml_request_name': 'job_name',
    'update_cp': False,
    'exchanges': None,
    'currency_pairs': None,
    'first_currencies': None,
    'second_currencies': None
}


class CreateConfig:

    def __init__(self, file: yaml, template_dict: Dict = template):
        self.file = file
        self.template = template_dict

    def blank_job(self):
        job_dict = {'blank': self.template}
        self.file['jobs'] = job_dict
        return self.file

    def reset_config(self):
        file = read_config("resources/templates/config_template.yaml")
        return file

    def example_ticker(self):
        self.template['yaml_request_name'] = 'ticker'
        self.template['exchanges'] = ['gateio']
        self.template['currency_pairs'] = ['all']

        self.file['jobs'] = {'ExampleTicker': self.template}

        return self.file

    def example_trades(self):
        self.template['yaml_request_name'] = 'trades'
        self.template['exchanges'] = ['gateio']
        self.template['currency_pairs'] = [{'first': 'btc', 'second': 'usdt'}]

        self.file['jobs'] = {'ExampleTrades': self.template}

        return self.file

    def example_order_books(self):
        self.template['yaml_request_name'] = 'order_books'
        self.template['exchanges'] = ['gateio']
        self.template['currency_pairs'] = [{'first': 'btc', 'second': 'usdt'}, {'first': 'eth', 'second': 'btc'}]

        self.file['jobs'] = {'ExampleOrderBooks': self.template}

        return self.file

    def example_historic_rates(self):
        self.template['yaml_request_name'] = 'historic_rates'
        self.template['exchanges'] = ['gateio']
        self.template['currency_pairs'] = None
        self.template['first_currencies'] = ['btc']

        self.file['jobs'] = {'ExampleHistoricRates': self.template}

        return self.file

    def example_ohlcvm(self):
        self.template['yaml_request_name'] = 'ohlcvm'
        self.template['exchanges'] = ['coingecko']
        self.template['currency_pairs'] = None
        self.template['first_currencies'] = ['btc']

        self.file['jobs'] = {'ExampleOHLCVM': self.template}

        return self.file


if __name__ == '__main__':
    config_file = read_config()

    ConfigCreator = CreateConfig(config_file)
    class_methods = [func for func in dir(ConfigCreator)
                     if callable(getattr(ConfigCreator, func))
                     and not func.startswith("__")]

    class_methods.append('csv_config')

    boolean = True
    while boolean:
        input_string = input(f"{class_methods} \n \n Select Method or exit/quit:")
        if input_string in class_methods:
            boolean = False
        elif input_string in ['quit', 'exit']:
            sys.exit(0)
        else:
            print("{} is no valid method. Try again.".format(input_string))

    if input_string == 'csv_config':
        file = read_config("/resources/templates/csv_config_template.yaml")
    else:
        method_to_call = getattr(ConfigCreator, input_string)
        file = method_to_call()
    write_config(file)
    print("Created new Config.")
