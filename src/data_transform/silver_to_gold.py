import os
import pandas as pd
import numpy as np
import time
import src.utils.Utils as utils

class SilverToGold:
    def __init__(self, silver_path, gold_path):
        self.silver_path = silver_path
        self.gold_path = gold_path
        self.silver_path_disaster = 'data/silver/base_disasrter'
    def process_generic(self, df):
        print("Processando dados genéricos...")
        # Implementar o tratamento genérico
        return df
    def process_base_1(self,df, type='daily'):
        pass
    def get_processor(self, subpasta):
        """
        Maps subfolders to processing functions.

        Parameters:
        - subfolder: Name of the subfolder to be processed.
        Returns:
        - the processing funcition corresponding to the subfolder.
        """
        processors = {
            #'base_disaster': self.process_base_disaster,
            #'base_1':self.process_base_1, # concluido
            'disaster':self.process_generic,
        }
        print(f"Processor for subfolder {subpasta}: {processors.get(subpasta, self.process_generic).__name__}")
        return processors.get(subpasta, self.process_generic)  # Função genérica por padrão

    def  process_subfolders(self):
        list_cities=['dallas','houston','miami','nashville','new york','oklahoma city','albuquerque','chicago']
        for subpasta in os.listdir(self.silver_path):
            subpasta_path = os.path.join(self.silver_path, subpasta)
            print(f'Analisando pasta: {subpasta_path} ...')
            if os.path.isdir(subpasta_path) and subpasta != 'base_disaster':
                list_files = os.listdir(subpasta_path)
                for city in list_cities:
                    print(f'Analisando cidade: {city} ...')
                    files  = [f for f in list_files if city in f]

                    file_disaster = [f for f in list_files if 'disaster' in f]
                    print(f'Arquivos encontrados: {files} ... \n') 
            else:
                print(f'{subpasta_path} não é uma pasta valida.')
                continue
if __name__ == '__main__':        
    # Uso da classe:
    silver_data_path = 'data/silver'
    output_data_path = 'data/gold'

    # Criar uma instância da classe
    processor = SilverToGold(silver_data_path, output_data_path)
    start_time = time.time()

    # Process the subfolders and files
    processor.process_subfolders()

    # Measure the end time of execution
    end_time = time.time()

    # Calculate and print the total execution time
    execution_time = end_time - start_time
    print(f"Execution Time: {execution_time:.2f} seconds")

                
