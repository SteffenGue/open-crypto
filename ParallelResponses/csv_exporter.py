from model.database.db_handler import DatabaseHandler
import pandas as pd


class CsvExporter:
    database_handler: DatabaseHandler
    csv_path: str
    job_name: str

    def __init__(self, csv_path: str, sep_symbol: str, job_name: str, database_handler: DatabaseHandler):
        self.database_handler = database_handler
        self.csv_path = csv_path
        self.job_name = job_name

    def makeDir(self) -> bool:
        #todo: make dir, if successful return true

    def create_csv(self):
        #todo: get readable data from database and put in csv

    # todo: typehint csv file
    def export_csv(self, csv_file):
        #todo: safe csv file in path