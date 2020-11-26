from ParallelResponses.tests.yaml_tests.api_map_validation import ApiMapFileValidator
from ParallelResponses.model.utilities.utilities import YAML_PATH
import os


class Validate:
    """
    This class will validate a single exchange yaml-file. The first method ValidateMapFile checks the yaml-file
    if in the right format, i.e. performs unit-testing.
    The second methods ValidateYAML plugs the yaml-file in a cooked-down version of the main program and checks
    the functionality.
    """

    def __init__(self, file_name: str):
        self.file_name = file_name

    def __ValidateMapFile__(self) -> bool:
        api_map = ApiMapFileValidator(YAML_PATH + self.file_name + '.yaml')

        api_map.validate()
        if api_map.report.__bool__():
            return True
        else:
            with open('tests/yaml_tests/reports/report_' + self.file_name + '.txt', 'w') as report:
                report.writelines(api_map.report.indented_report())
                report.close()
            print("API Map is Invalid! \n"
                  f"Inspect report: {'~/reports/report_' + self.file_name + '.txt'}")
            self.report_error(api_map.report)
            return False

    def validate(self):

        Map = self.__ValidateMapFile__()
        if Map:
            print(f"Exchange '{self.file_name}' valid: True")
            return True
        else:
            return False

    def report_error(self, report):
        """Recursive method to find the lowest False in a nested list"""
        if report.__bool__():
            # if report is True, pass
            return report
        #if report is False:
        else:
            # look at nested reports,
            for nested_report in report.reports:
               # if report is false
                if not nested_report.__bool__():
                    try:
                        self.report_error(nested_report)
                    # break if no further nested reports can be found, i.e. AttributeError
                    except AttributeError:
                        print(nested_report)
                        break

if __name__ == '__main__':
    exchange = input("Enter 'Exchange Name' or 'all': \n")
    if exchange == 'all':
        exchanges = os.listdir(YAML_PATH)
        exchanges = [exchange.split('.yaml')[0] for exchange in exchanges]
        valid = 0
        for exchange in exchanges:
            valid += Validate(exchange).validate()

        print(f'Valid Exchanges: {round(valid/len(exchanges) *100, 2)} %')
    else:
        Validate(exchange).validate()



