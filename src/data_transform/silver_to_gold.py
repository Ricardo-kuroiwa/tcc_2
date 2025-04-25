import os
import pandas as pd
import numpy as np
import time
from collections import defaultdict
import src.utils.Utils as utils

class SilverToGold:
    def __init__(self, silver_path, gold_path):
        self.silver_path = silver_path
        self.gold_path = gold_path
        self.silver_path_disaster = 'data/silver/base_disaster'
    def get_disaster(self, city_name):
        files_in_path =os.listdir(self.silver_path_disaster)
        path_file_disaster =[file for file in files_in_path if city_name in file]
        return path_file_disaster[0] if path_file_disaster else None
    
    def process_generic(self,files_path,file_disaster ,subpasta):
        print("Processando dados genéricos...")
        # Implementar o tratamento genérico
        return None
    
    def process_base_1(self,files_path,file_disaster ,subpasta):
        '''
        Processing function for base_1 subfolder.
        This function processes the data files found in the specified subfolder and merges them with disaster data.
        Args:
            files_path (list): List of file paths to be processed.
            file_disaster (str):  Disaster file name.
            subpasta (str): Subfolder name.
        '''
        for file in files_path:
            if 'daily' in file:
                print(f'    📄 Arquivo diário encontrado: {file}')
                df_daily = utils.read_data_from_parquet(os.path.join(self.silver_path,subpasta, file))
            else : 
                print(f'    📄 Arquivo horário encontrado: {file}'  )
                df_hourly = utils.read_data_from_parquet(os.path.join(self.silver_path,subpasta, file))
        print(f'    📄 Arquivo desastre encontrado: {file_disaster}')
        df_disaster = utils.read_data_from_parquet(os.path.join(self.silver_path_disaster, file_disaster))
        df_disaster = df_disaster.drop(columns=['location'])
        df_daily['season'] = df_daily['date'].apply(utils.get_season)

        medias_por_dia = df_hourly.groupby('date').mean(numeric_only=True).reset_index()

        base_completa = pd.merge(df_daily, medias_por_dia, on='date', how='left')

        base_final = pd.merge(base_completa, df_disaster, on='date', how='outer')
        base_final['disaster_occurred']= base_final['eventType'].apply(lambda x:  0 if pd.isna(x) else 1)
        print(f'Top 10 rows of base_final:')
        print(base_final.head(10))
        return base_final
    
    def process_base_2(self,files_path,file_disaster ,subpasta):
        '''
        Processing function for base_1 subfolder.
        This function processes the data files found in the specified subfolder and merges them with disaster data.
        Args:
            files_path (list): List of file paths to be processed.
            file_disaster (str):  Disaster file name.
            subpasta (str): Subfolder name.
        '''
        for file in files_path:
            print(f'    📄 Arquivo diário encontrado: {file}')
            data = utils.read_data_from_parquet(os.path.join(self.silver_path,subpasta, file))
        print(f'    📄 Arquivo desastre encontrado: {file_disaster}')
        df_disaster = utils.read_data_from_parquet(os.path.join(self.silver_path_disaster, file_disaster))
        df_disaster = df_disaster.drop(columns=['location'])
        data['season'] = data['date'].apply(utils.get_season)
        base_final = pd.merge(data, df_disaster, on='date', how='outer')
        base_final['disaster_occurred']= base_final['eventType'].apply(lambda x:  0 if pd.isna(x) else 1)
        #print(f'Top 10 rows of base_final:')
        #print(base_final.head(10))

        return base_final

    def process_base_3(self,files_path,file_disaster ,subpasta):
        for file in files_path:
            if 'daily' in file:
                print(f'    📄 Arquivo diário encontrado: {file}')
                df_daily = utils.read_data_from_parquet(os.path.join(self.silver_path,subpasta, file))
            else : 
                print(f'    📄 Arquivo horário encontrado: {file}'  )
                df_hourly = utils.read_data_from_parquet(os.path.join(self.silver_path,subpasta, file))
        print(f'    📄 Arquivo desastre encontrado: {file_disaster}')
        df_disaster = utils.read_data_from_parquet(os.path.join(self.silver_path_disaster, file_disaster))
        df_disaster = df_disaster.drop(columns=['location'])
        medias_por_dia = df_hourly.groupby('date').mean(numeric_only=True).reset_index()
        medias_por_dia = medias_por_dia[['date','dewpoint','relative_humidity','wind_direction','wind_speed','precipitation']]
        df_merged = pd.merge(df_daily, medias_por_dia, on=['precipitation', 'wind_direction', 'wind_speed'], how='left', suffixes=('_df1', '_df2'))
        df_daily['precipitation'] = df_daily['precipitation'].fillna(df_merged['precipitation'])
        df_daily['wind_direction'] = df_daily['wind_direction'].fillna(df_merged['wind_direction'])
        df_daily['wind_speed'] = df_daily['wind_speed'].fillna(df_merged['wind_speed'])
        df_daily =df_daily.drop(columns=['total_sunshine_duration','wind_gust'])
        medias_por_dia = medias_por_dia.drop(columns=['wind_direction','precipitation','wind_speed'])
        #print(df_daily.columns)
        base_completa = pd.merge(df_daily, medias_por_dia, on='date', how='left')
        percentual_nulo = utils.count_null_values(base_completa)
        cols_to_interpolate = percentual_nulo [(percentual_nulo  <= 40) & (percentual_nulo>0)].index
        #print(f"Columns to interpolate: {cols_to_interpolate}")
        for col in cols_to_interpolate:
            base_completa[col] = base_completa[col].interpolate()
        #print("After interpolation:")
        
        #print(utils.count_null_values(base_completa))
        
        base_final = pd.merge(base_completa, df_disaster, on='date', how='outer')
        base_final['disaster_occurred']= base_final['eventType'].apply(lambda x:  0 if pd.isna(x) else 1)
        #print(f'Top 10 rows of base_final:')
        #print(base_final.head(10))
        
        return base_completa

    def get_processor(self, subpasta):
        """
        Maps subfolders to processing functions.

        Parameters:
        - subfolder: Name of the subfolder to be processed.
        Returns:
        - the processing funcition corresponding to the subfolder.
        """
        processors = {
            'base_1':self.process_base_1, # concluido
            'base_2':self.process_base_2, # concluido
            'base_3':self.process_base_3 # concluido

        }
        print(f"Processor for subfolder {subpasta}: {processors.get(subpasta, self.process_generic).__name__}")
        return processors.get(subpasta, self.process_generic)  # Função genérica por padrão

    def process_subfolders(self):
            list_cities = [
                'dallas', 'houston', 'miami', 'nashville',
                'new york', 'oklahoma city', 'albuquerque', 'chicago'
            ]

            for subpasta in os.listdir(self.silver_path):
                subpasta_path = os.path.join(self.silver_path, subpasta)
                print(f'📁 Analisando pasta: {subpasta_path} ...')

                if os.path.isdir(subpasta_path) and subpasta != 'base_disaster':
                    list_files = os.listdir(subpasta_path)

                    # Cria um dicionário que agrupa arquivos por cidade
                    city_files = defaultdict(list)
                    for f in list_files:
                        for city in list_cities:
                            if city.lower() in f.lower():
                                city_files[city].append(f)

                    # Agora só percorre as cidades com arquivos encontrados
                    for city, files in city_files.items():
                        file_disaster = self.get_disaster(city)
                        print(f' {city} :')
                        print(f'    📄 Arquivos encontrados : {files}')
                        print(f'    📄 Arquivo desastre : {file_disaster}\n')
                        processor= self.get_processor(subpasta)
                        processed_data= processor(files ,file_disaster,subpasta)
                        file_path_to_save = f'{self.gold_path}/{subpasta}/{city}_1973_2024.parquet'
                        print(f'    📁 Salvando arquivo em: {file_path_to_save}')
                        utils.save_data_to_parquet(processed_data, file_path_to_save)
                else:
                    print(f'🚫 {subpasta_path} não é uma pasta válida (ou é base_disaster).')
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

                
