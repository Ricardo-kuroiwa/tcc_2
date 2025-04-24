import os
import json
import time
import pandas as pd
import numpy as np

class DataProcessor:
    def __init__(self, raw_data_path, output_data_path, file_path_mapping_column):
        self.raw_data_path = raw_data_path
        self.output_data_path = output_data_path
        self.map_column = self.load_mapping_column(file_path_mapping_column)
        # Criar o diretório de saída, se necessário
        os.makedirs(self.output_data_path, exist_ok=True)
    
    def read_file(self, file_path):
        """
        Reads a CSV file into a DataFrame,Skipping bad lines.

        Parameters:
        -  file_path: Path to the CSV file to read.

        Returns:
        - DataFrame containing the data from the CSV file.

        """
        try:
            return pd.read_csv(file_path,on_bad_lines='skip')
        except Exception as e:
            print(f"Error reading file {file_path}: {e}")
            return None
    
    def load_mapping_column(self, file_path):
        """
        Load the column mapping from a JSON file.
        Parameters:
        - file_path :Path to the JSON file containing the column mapping.
        Returns:
        - Dictionary containing the column mapping.
        """
        with open(file_path, 'r') as file:
            data = json.load(file)
            return data

    def save_to_parquet(self, df, file_path):
        """
        Saves a DataFrame to a Parquet file.

        Parameters:
        - df:DataFrame to be saved.
        - file_path: Path to the Parquet file to be saved. 
        """
        try:
            df.to_parquet(file_path, index=False, engine='pyarrow', compression='snappy')
            print(f"File saved to {file_path}")
        except Exception as e:
            print(f"Error saving file {file_path}: {e}")
    
    def count_null_values(self,df):
        """
        Prints the percentage of null values in each column of DataFrame.
        Parameters:

        - df: DataFrame to be analyzed.
        """
        percentual_nulos =( df.isnull().sum() / len(df)) * 100
        percentual_nulos = percentual_nulos.round(2).sort_values(ascending=False)
        print("Null value percentages:")
        # Exibir os resultados
        print(percentual_nulos)
        return percentual_nulos
    
    def show_distinct_values(self, df):
        """
        Displays the distinct values in the selected columns of a DataFrame.
        Parameters:
        - df: DataFrame to be analyzed.
        """
        print("Counting distinct values...")
        select_columns = ['CZ_NAME_STR']

        # Verificar se as colunas selecionadas estão no DataFrame
        valid_columns = [col for col in select_columns if col in df.columns]
    
        if valid_columns:
            for col in valid_columns:
                print(f"Distinct values in column {col}: {len(df[col].unique())}")
                print(df[col].unique()) 
        else:
            print("None of the selected columns are present in the DataFrame.")

    def process_base_1(self,df):
        print("Processing  base 3 data...")
        print(f"Number of rows: {df.shape[0]}")
        df = df.loc[:, :]
        df = self.rename_collumns(df, 'base_1')
        df = self.convert_to_datetime(df,'date')
        print("Displaying the first  rows of the DataFrame...")
        print(df.head(3))
        num_duplicates = df.duplicated(subset=['date'], keep=False).sum() 
        print(f"Number of duplicates: {num_duplicates}")
        duplicates = df[df.duplicated(subset=['date'], keep=False)] 
        print(duplicates)
        self.count_null_values(df)
        
        return df
    
    
    def process_base_2(self, df):
        print("Processing  base 2 data...")
        print(f"Number of rows: {df.shape[0]}")
        #print(df.columns)
        select_columns = [ 'DATE', 'AWND', 'PRCP', 'SNOW', 'SNWD',
       'TAVG', 'TMAX', 'TMIN', 'WDFG', 'WSFG']
        valid_columns = [col for col in select_columns if col in df.columns]
        df = df.loc[:, valid_columns]
        df = self.rename_collumns(df, 'base_2')
        df = self.convert_to_datetime(df,'date')
        print(f'type : {df['date'].dtype}')
        #print("Displaying the first  rows of the DataFrame...")
        #print(df.head(3))
        num_duplicates = df.duplicated(subset=['date'], keep=False).sum() 
        #print(f"Number of duplicates: {num_duplicates}")
        duplicates = df[df.duplicated(subset=['date'], keep=False)] 
        #print(duplicates)
        if 'average_temperature' not in df.columns:
            df['average_temperature'] = (df['maximum_temperature'] + df['minimum_temperature']) / 2
        else:
            df['average_temperature'] = np.where(
            df['average_temperature'].isnull(),
            (df['maximum_temperature'] + df['minimum_temperature']) / 2,
            df['average_temperature']
        )
        df.drop(columns=['wind_gust','wind_direction','wind_speed'], inplace=True,errors='ignore')
        percentual_nulo = self.count_null_values(df)
        cols_to_interpolate = percentual_nulo [(percentual_nulo  <= 20) & (percentual_nulo>0)].index
        print(f"Columns to interpolate: {cols_to_interpolate}")
        for col in cols_to_interpolate:
            df[col] = df[col].interpolate()
        print("After interpolation:")
        percentual_nulo = self.count_null_values(df)
        print("\n")

        return df

    def process_base_3(self, df):
        print("Processing  base 3 data...")
        print(f"Number of rows: {df.shape[0]}")
        df = df.loc[:, :]
        df = self.rename_collumns(df, 'base_3')
        df = self.convert_to_datetime(df,'date')
        print("Displaying the first  rows of the DataFrame...")
        print(df.head(3))
        num_duplicates = df.duplicated(subset=['date'], keep=False).sum() 
        print(f"Number of duplicates: {num_duplicates}")
        duplicates = df[df.duplicated(subset=['date'], keep=False)] 
        print(duplicates)
        self.count_null_values(df)
        percentual_nulo = self.count_null_values(df)
        cols_to_interpolate = percentual_nulo [(percentual_nulo  <= 20) & (percentual_nulo>0)].index
        print(f"Columns to interpolate: {cols_to_interpolate}")
        for col in cols_to_interpolate:
            df[col] = df[col].interpolate()
        print("After interpolation:")
        percentual_nulo = self.count_null_values(df)
        print("\n")
        print("\n")
        return df
    
    def rename_collumns(self,df, type_data='base_disaster'):
        """
        Renames the columns of a DataFrame based on the mapping.

        Parameters:
        - df: DataFrame to be processed.
        - type_data: Type of data to be processed. Default is 'base_disaster'.
        Returns:
        - DataFrame with the columns renamed.
        """

        print("Renaming columns...")

        # Check if the type_data key exists in self.map_column
        if type_data in self.map_column:
            try:
                # Renaming columns based on the mapping
                df.rename(columns=self.map_column[type_data], inplace=True)
                print(f"Renaming successful. New columns: {df.columns}")
            except Exception as e:
                print(f"Error renaming columns: {e}")
        else:
            print(f"Data type '{type_data}' not found in the mapping.")
        return df
    
    def convert_to_datetime(self,df,column_name):
        """
        Converts a column to a datetime format.
        Parameters:
        - df : DataFrame to be processed.
        - column_name: Name of the column to be converted.
        Returns:
        - DataFrame with the column converted to datetime format.
        """
        formats = ['%m/%d/%Y', '%Y-%m-%d', '%d/%m/%Y']  
        df[column_name] = df[column_name].astype(str).str.split(' ').str[0] 
        for fmt in formats:
            try:
                df[column_name] = pd.to_datetime(df[column_name], format=fmt)
                return df
            except Exception as e:
                #print(f"Error converting to datetime: {e}")
                continue  
        
        return df 
    
    def process_base_disaster(self,df):
        """
        Processes the base_disaster data,incluind renaming columns,
        converting dates, andhandling duplicates.
        Parameters:
        - df: DataFrame to be processed.
        Returns:
        - Processed DataFrame.
        """
        print("Processing disaster data...")
        print(f"Number of rows: {df.shape[0]}")
        select_columns =['CZ_NAME_STR','BEGIN_DATE','EVENT_TYPE']
        df = df.loc[:, select_columns]
        df = df.dropna()
        df = self.rename_collumns(df,"base_disaster")
        df = self.convert_to_datetime(df,'date')
        print("Displaying the first  rows of the DataFrame...")
        print(df.head(3))
        num_duplicates = df.duplicated(subset=['date'], keep=False).sum()  # Conta quantas duplicatas existem
        print(f"Number of duplicates: {num_duplicates}")
        duplicates = df[df.duplicated(subset=['date'], keep=False)] # Mostra todas as duplicatas
        print(duplicates)
        df = df.drop_duplicates(subset=['date','eventType'], keep='first')  # Remove duplicatas, mantendo a primeira ocorrência
        self.count_null_values(df)
        print("\n")
        return df
    
    def process_generic(self, df):
        print("Processando dados genéricos...")
        # Implementar o tratamento genérico
        return df
    
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
            'base_2': self.process_base_2,
            'base_3': self.process_base_3, # concluido
            'base_disaster': self.process_base_disaster, # concluido
            'disaster':self.process_generic,
        }
        print(f"Processor for subfolder {subpasta}: {processors.get(subpasta, self.process_generic).__name__}")
        return processors.get(subpasta, self.process_generic)  # Função genérica por padrão

    def process_subfolders(self):
        """
        Process all subfolders and files in the raw data directory
        for each subfolder, the corresponding processing function is called.
        """
        for subpasta in os.listdir(self.raw_data_path):
            subpasta_path = os.path.join(self.raw_data_path, subpasta)
            
            if os.path.isdir(subpasta_path):
                print(f"Processing subfolder {subpasta_path}...")

                # Criar a pasta de saída para a subpasta
                output_subpasta_path = os.path.join(self.output_data_path, subpasta)
                os.makedirs(output_subpasta_path, exist_ok=True)

                # Obter a função de processamento com base no nome da subpasta
                processor = self.get_processor(subpasta)

                # Processar os arquivos na subpasta
                for file in os.listdir(subpasta_path):
                    file_path = os.path.join(subpasta_path, file)
                    if file.endswith('.csv'):
                        print(f"Processing file {file_path}")
                        df = self.read_file(file_path)
                        if df is not None:
                            # Aplicar o tratamento específico para a subpasta
                            df = processor(df)
                            # Salvar o arquivo em Parquet
                            self.save_to_parquet(df, os.path.join(output_subpasta_path, file.replace('.csv', '.parquet')))

if __name__ == '__main__':        
    # Uso da classe:
    raw_data_path = 'data/raw'
    output_data_path = 'data/silver'
    file_path_mapping_column = 'configs/dataframe_column_mapping.json'

    # Criar uma instância da classe
    processor = DataProcessor(raw_data_path, output_data_path,file_path_mapping_column)
    #print(vars(processor))

    start_time = time.time()

    # Process the subfolders and files
    processor.process_subfolders()

    # Measure the end time of execution
    end_time = time.time()

    # Calculate and print the total execution time
    execution_time = end_time - start_time
    print(f"Execution Time: {execution_time:.2f} seconds")
