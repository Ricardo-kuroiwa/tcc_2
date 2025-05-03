import src.train.decision_tree as decision_tree
import src.train.lightgbm_model as lightgbm_model
import src.train.xgboost_model as xgboost_model
import src.utils.Utils as utils  
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
import pandas as pd
import os 

# Listas de cidades para cada base
cities_base_1 = ['albuquerque', 'miami', 'chicago']
cities_base_2 = ['albuquerque', 'dallas', 'oklahoma city']
cities_base_3 = ['albuquerque', 'nashville', 'chicago']

# Dicionários para armazenar os DataFrames de cada base
dataframe_base_1 = {}
dataframe_base_2 = {}
dataframe_base_3 = {}

# Dicionário que mapeia as bases para as listas de cidades
base_to_cities = {
    'base_1': cities_base_1,
    'base_2': cities_base_2,
    'base_3': cities_base_3,
}
Balancing_Methods = [None,'SMOTE', 'ADASYN', 'RandomUnderSampler', 'SMOTEENN']
def train_model(df,city,base):
    X_train, X_test, y_train, y_test = train_test_split(df.drop(columns=['disaster_occurred']), df['disaster_occurred'], test_size=0.3, random_state=42,stratify=df['disaster_occurred'])
    #print(f"X_train: {X_train.shape}, y_train: {y_train.shape}, X_test: {X_test.shape}, y_test: {y_test.shape}")
    print(f"base: {base}, city: {city}, column_target: {y_test.name}")
    '''
    decision_tree.train_decision_tree(X_train, y_train, X_test, y_test,y_test.name,base,city)
    lightgbm_model.train_lightgbm(X_train, y_train, X_test, y_test, y_test.name,base,city)
    xgboost_model.train_xgboost(X_train, y_train, X_test, y_test, y_test.name,base,city)'''
    for method in Balancing_Methods:
        decision_tree.train_decision_tree(X_train, y_train, X_test, y_test,y_test.name,base,city,method)
        lightgbm_model.train_lightgbm(X_train, y_train, X_test, y_test, y_test.name,base,city,method)
        xgboost_model.train_xgboost(X_train, y_train, X_test, y_test, y_test.name,base,city,method)
    print("Success in training models")

def load_data_for_base(file_path, city, base, dataframe_dict):
    """Função que carrega o DataFrame para a base correspondente."""
    if city in base_to_cities[base]:
        df = utils.read_data_from_parquet(file_path)
        dataframe_dict[city] = df
        print(f"DataFrame para cidade {city} carregado na {base}.")
def preprocess_dataframe(df):
    scaler = StandardScaler()
    encoder = OneHotEncoder(sparse_output=False) 

    cols_to_exclude = ['date', 'season', 'eventType', 'disaster_occurred']
    cols_to_scale = [col for col in df.columns if col not in cols_to_exclude]

    season_encoded = encoder.fit_transform(df[['season']])
    df_scaled_values = scaler.fit_transform(df[cols_to_scale])
    
    df_scaled = pd.DataFrame(df_scaled_values, columns=cols_to_scale, index=df.index)
    season_encoded_df = pd.DataFrame(season_encoded, columns=encoder.get_feature_names_out(['season']), index=df.index)

    df_final = pd.concat([df[cols_to_exclude], df_scaled, season_encoded_df], axis=1)
    df_final = utils.extract_date_components(df_final, 'date')
    df_final = df_final.drop(columns=['date', 'eventType', 'season'])

    return df_final


if __name__ == "__main__":
    gold_data_path = 'data/gold'
    
    for dir in os.listdir(gold_data_path):
        file_dir = os.path.join(gold_data_path, dir)
        print(f'Analisando diretório: {file_dir} ...\n')
        
        for paths in os.listdir(file_dir):
            file_path = os.path.join(file_dir, paths)
            city = paths.split('_')[0]
            print(f'Analisando cidade: {city} ...')
            print(f'Arquivo: {paths} ... \n')

            match dir:
                case 'base_1':
                    load_data_for_base(file_path, city, 'base_1', dataframe_base_1)
                case 'base_2':
                    load_data_for_base(file_path, city, 'base_2', dataframe_base_2)
                case 'base_3':
                    load_data_for_base(file_path, city, 'base_3', dataframe_base_3)
                case _:
                    pass

        print("-----/-----/"*10)

    print(f"DataFrames para a base 1: {len(dataframe_base_1)}")
    print(f"DataFrames para a base 2: {len(dataframe_base_2)}")
    print(f"DataFrames para a base 3: {len(dataframe_base_3)}")
    print(f'Chaves em dataframe_base_1: {list(dataframe_base_1.keys())}')
    print(f'Chaves em dataframe_base_2: {list(dataframe_base_2.keys())}')
    print(f'Chaves em dataframe_base_3: {list(dataframe_base_3.keys())}')
    list_dataframes = [dataframe_base_1, dataframe_base_2, dataframe_base_3]
    
    for base_name, dataframe_dict in zip(['base_1', 'base_2', 'base_3'], list_dataframes):
        for city, df in dataframe_dict.items():
            print(f'Treinando modelos para {city} na {base_name}...')
            df_final = preprocess_dataframe(df)
            train_model(df_final, city, base_name)




    