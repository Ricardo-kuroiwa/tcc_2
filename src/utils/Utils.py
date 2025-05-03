import os
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
import abc

def extract_date_components(df: pd.DataFrame, date_column: str) -> pd.DataFrame:
    if date_column not in df.columns:
        raise ValueError(f"Column '{date_column}' not found in DataFrame.")
    df[date_column] = pd.to_datetime(df[date_column])
    df['year'] = df[date_column].dt.year
    df['month'] = df[date_column].dt.month
    df['day'] = df[date_column].dt.day

    return df

def count_null_values(dataframe: pd.DataFrame) -> pd.Series:    
        """
        Prints the percentage of null values in each column of DataFrame.
        Parameters:

        - df: DataFrame to be analyzed.
        """
        percentual_nulos =( dataframe.isnull().sum() / len(dataframe)) * 100
        percentual_nulos = percentual_nulos.round(2).sort_values(ascending=False)
        #print("Null value percentages:")
        # Exibir os resultados
        #print(percentual_nulos)
        return percentual_nulos

def get_season(date: pd.Timestamp) -> str:
    """
    Deterninate the season based on the month of the date.for northern hemisphere.
    Args:
        date (datetime): Date to determine the season.
    Returns:
            str: Season name.
    """
    month = date.month
    if month in [12, 1, 2]:
        return 'Inverno'
    elif month in [3, 4, 5]:
        return 'Primavera'
    elif month in [6, 7, 8]:
        return 'Verão'
    else:  
        return 'Outono'

def add_moving_average(df, column: str, window: int = 3)-> pd.DataFrame: 
    """add moving average columns to the dataframe
    Args:
        df (pd.DataFrame): DataFrame to add moving average columns.
        column (str): Column name to calculate moving average.
        window (int): Window size for moving average.
    Returns:
        pd.DataFrame: DataFrame with moving average columns added.
    """
    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found in DataFrame.")
    df[f'{column}_moving_average_sma'] = df[column].rolling(window=window).mean()
    df[f'{column}_moving_average_ema'] =  df[column].ewm(span=window, adjust=False).mean()
    return df

def save_plot(folder: str, filename: str) -> None:
    """"Save the plot to a specified folder and filename.
    Args:
        folder (str): Folder name to save the plot.
        filename (str): Filename to save the plot.
    """ 
    path = os.path.join('figures', folder)
    if not os.path.exists(path):
        os.makedirs(path)
    plt.tight_layout() 
    plt.savefig(os.path.join(path, filename), bbox_inches='tight')
    plt.close()

def read_data_from_parquet( file_path: str)-> pd.DataFrame:
    """Read data from a parquet file.   
    Args:
        file_path (str): Path to the parquet file.
    Returns:    
        pd.DataFrame: DataFrame containing the data from the parquet file.
    """
    try:
        return pd.read_parquet(file_path)
    except Exception as e:
        print(f"Erro ao ler o arquivo {file_path}: {e}")
        return None

def save_data_to_parquet(df: pd.DataFrame, file_path: str)-> None:
    """Save data to a parquet file.
    Args:
        df (pd.DataFrame): DataFrame to save.
        file_path (str): Path to save the parquet file.
    """
    dir_path = os.path.dirname(file_path)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    try:
        df.to_parquet(file_path, index=False)
    except Exception as e:
        print(f"Erro ao salvar o arquivo {file_path}: {e}")