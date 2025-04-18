import os
import re
import pandas as pd
import src.utils.Utils as utils  

class Base1Analysis:
    def __init__(self, path):
        self.path = path
        self.city = None
    def analysis_sub_folders(self):
        print(f'Analisando pasta: {self.path} ...')
        dfs_by_city = {}

        for file in os.listdir(self.path):
            if file.endswith('.parquet'):
                file_path = os.path.join(self.path, file)
                city =  file.split('_')[0]
                print(f'Analisando cidade: {city} ...')
                print(f'Arquivo: {file} ... \n')
        
                df = utils.read_data_from_parquet(file_path)

if __name__ == "__main__":
    # Example usage
    silver_data_path = 'data/silver/base_1'
    analysis = Base1Analysis(silver_data_path)
    analysis.analysis_sub_folders()