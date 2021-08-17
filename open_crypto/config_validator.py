from model.validating.config_file_validator import ConfigFileValidator
import os

path = os.getcwd() + "/resources/configs/user_configs/Examples/trades.yaml"

validator = ConfigFileValidator(path)


if __name__ == '__main__':
    validator.validate()



