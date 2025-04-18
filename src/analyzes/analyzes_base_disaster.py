import os
import re
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import src.utils.Utils as utils




class DisasterAnalysis:
    def __init__(self, path):
        self.path = path
        self.city = None
    def show_analysis_base_disaster(self, df):
        print(f"\n--- Análise base de desastres ---")
        print(f"Cidade: {self.city}")
        self.show_basic_data(df)
        self.show_missing_values(df)
        self.show_disaster_by_year(df)
        self.show_disaster_by_event_type(df)
        self.show_disaster_by_season(df)
        self.show_event_type_over_years(df)
        self.show_disaster_over_years(df)

    def show_basic_data(self, df):
        print(df.head(3))
        df.info()

    def show_missing_values(self, df):
        percentual_nulos = (df.isnull().sum() / len(df)) * 100
        print("Percentual de valores nulos:")
        print(percentual_nulos)

    def show_event_type_over_years(self, df):
        df['year'] = pd.to_datetime(df['date'], errors='coerce').dt.year
        pivot = df.pivot_table(index='year', columns='eventType', aggfunc='size', fill_value=0)

        pivot.plot(kind='line', figsize=(14, 6), marker='o')
        plt.title(f'Evolução dos tipos de desastre por ano - {self.city}')
        plt.ylabel('Quantidade')
        plt.xlabel('Ano')
        plt.grid(True)
        plt.tight_layout()
        utils.save_plot('base_disaster/event_type_over_years', f'event_type_over_years_{self.city}.png')
    def show_disaster_over_years(self, df):
        # Conversão de data para ano
        df['year'] = pd.to_datetime(df['date'], errors='coerce').dt.year

        # Criar lista de todos os anos possíveis no intervalo
        all_years = pd.Series(range(df['year'].min(), df['year'].max() + 1), name='year')
        df_grouped = df.groupby('year').size().reset_index(name='count')
        df_grouped = pd.merge(all_years, df_grouped, on='year', how='left')
        df_grouped['count'] = df_grouped['count'].fillna(0).astype(int)
        df_grouped['year'] = df_grouped['year'].astype(int)
        df_grouped = df_grouped.sort_values('year')

        # Adicionar médias móveis (SMA e EMA)
        window_size = 3
        df_grouped = utils.add_moving_average(df_grouped, column='count', window=window_size) # Média móvel simples e exponencial

        # Criar gráfico
        fig, ax = plt.subplots(figsize=(10, 6))
        
        ax.bar(df_grouped['year'], df_grouped['count'], color='skyblue', label='Desastres')
        ax.plot(df_grouped['year'], df_grouped['count_moving_average_sma'], color='red', label='Média Móvel Simples (SMA)', linewidth=2.5)
        ax.plot(df_grouped['year'], df_grouped['count_moving_average_ema'], color='green', label='Média Móvel expodencial (EMA)', linewidth=2.5, linestyle='--')


        # Ajustes no gráfico
        ax.set_title(f'Desastres por ano - {self.city}')
        ax.set_xlabel('Ano')
        ax.set_ylabel('Quantidade de desastres')
        plt.xticks(rotation=45)
        plt.legend()
        # Salvar o gráfico
        utils.save_plot('base_disaster/event_over_years', f'event_over_years_{self.city}.png')


    def show_disaster_by_year(self, df):
        if 'date' not in df.columns:
            print("Coluna 'date' não encontrada.")
            return
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['year'] = df['date'].dt.year
        disaster_by_year = df.groupby('year').size().reset_index(name='count')

        plt.figure(figsize=(10, 5))
        ax =sns.barplot(data=disaster_by_year, x='year', y='count')
        ax.set_title(f'Desastres por ano - {self.city}')
        ax.set_xlabel('Ano')
        ax.set_ylabel('Quantidade de desastres')
        plt.xticks(rotation=45)
        for p in ax.patches:
            height = p.get_height()
            ax.text(x=p.get_x() + p.get_width() / 2,
                y=height + 0.02 * height,
                s=int(height),
                ha='center')
        utils.save_plot('base_disaster/disaster_by_year', f'disaster_by_year_{self.city}.png')

    def show_disaster_by_event_type(self, df):
        if 'eventType' not in df.columns:
            print("Coluna 'eventType' não encontrada.")
            return
        disaster_by_type = df['eventType'].value_counts().sort_values(ascending=True)

        plt.figure(figsize=(10, 5))
        sns.barplot(x=disaster_by_type.values, y=disaster_by_type.index)
        plt.title(f'Desastres por tipo de evento - {self.city}')
        plt.xlabel('Quantidade de desastres')
        plt.ylabel('Tipo de evento')
        utils.save_plot('base_disaster/disaster_by_event_type', f'disaster_by_event_type_{self.city}.png')

    

    def show_count_disaster_by_city(self, dfs_by_city):
        disaster_count = {city: df.shape[0] for city, df in dfs_by_city.items()}
        df = pd.DataFrame.from_dict(disaster_count, orient='index', columns=['count']).sort_values(by='count')

        plt.figure(figsize=(10, 5))
        sns.barplot(x=df['count'], y=df.index)
        plt.title('Desastres por cidade')
        plt.xlabel('Quantidade de desastres')
        plt.ylabel('Cidade')
        utils.save_plot('base_disaster/count_disaster_by_city', 'count_disaster_by_city.png')
        
    
    def show_disaster_by_season(self, df):
        if 'date' not in df.columns:
            print("Coluna 'date' não encontrada.")
            return
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['season'] = df['date'].apply(utils.get_season)
        disaster_by_season = df.groupby('season').size().reset_index(name='count')
        print(disaster_by_season.head(3))
        
        plt.figure(figsize=(10, 5))
        ax = sns.barplot(data=disaster_by_season, x='season', y='count',hue='season', palette="hls")
        ax.set_title(f'Desastres por estação - {self.city}')
        ax.set_xlabel('Estação')
        ax.set_ylabel('Quantidade de desastres')
        for p in ax.patches:
            height = p.get_height()
            ax.text(x=p.get_x() + p.get_width() / 2,
                y=height + 0.02 * height,
                s=int(height),
                ha='center')
        utils.save_plot('base_disaster/disaster_by_season', f'disaster_by_season_{self.city}.png')
    # principal function 
    def analysis_sub_folders(self):
        print(f'Analisando pasta: {self.path} ...')
        dfs_by_city = {}

        for file in os.listdir(self.path):
            if file.endswith('.parquet'):
                file_path = os.path.join(self.path, file)
                city = file.split('_')[0]
                df = utils.read_data_from_parquet(file_path)
                if df is not None:
                    dfs_by_city[city] = df

        self.show_count_disaster_by_city(dfs_by_city)

        for city, df in dfs_by_city.items():
            self.city = city
            self.show_analysis_base_disaster(df)
            print('\n')


if __name__ == '__main__':
    silver_data_path = 'data/silver/base_disaster'
    analysis = DisasterAnalysis(silver_data_path)
    analysis.analysis_sub_folders()
