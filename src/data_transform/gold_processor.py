import os
import time
import pandas as pd
from pathlib import Path
from collections import defaultdict
import src.utils.Utils as utils
from src.utils.logger import setup_logger

logger = setup_logger("gold_processor")


class GoldProcessor:
    """
    Camada Gold: agrupa, enriquece e une os dados limpos com dados de desastres.
    Saída final é um dataset por base e por cidade com a feature 'disaster_occurred'.
    """

    def __init__(self, silver_path: str, gold_path: str):
        self.silver_path = Path(silver_path)
        self.gold_path = Path(gold_path)
        self.silver_path_disaster = self.silver_path / "base_disaster"
        self.cities = [
            'dallas', 'houston', 'miami', 'nashville',
            'new york', 'oklahoma city', 'albuquerque', 'chicago'
        ]

    def get_disaster_file_for_city(self, city_name: str) -> Path:
        """Busca o arquivo de desastre correspondente a uma cidade."""
        for file in os.listdir(self.silver_path_disaster):
            if city_name.lower() in file.lower():
                return self.silver_path_disaster / file
        logger.warning(f"Nenhum arquivo de desastre encontrado para {city_name}")
        return None

    def process_base_1(self, city_files, disaster_file, base_name):
        """Processamento específico da base 1 (diário + horário)."""
        df_daily, df_hourly = None, None

        for file in city_files:
            file_path = self.silver_path / base_name / file
            if "daily" in file:
                df_daily = utils.read_data_from_parquet(file_path)
                logger.info(f"Leitura de arquivo diário: {file}")
            elif "hourly" in file:
                df_hourly = utils.read_data_from_parquet(file_path)
                logger.info(f"Leitura de arquivo horário: {file}")

        df_disaster = utils.read_data_from_parquet(disaster_file).drop(columns=['location'], errors='ignore')

        df_daily['season'] = df_daily['date'].apply(utils.get_season)
        medias_por_dia = df_hourly.groupby('date').mean(numeric_only=True).reset_index()
        df_base = pd.merge(df_daily, medias_por_dia, on='date', how='left')
        df_final = self.merge_with_disasters(df_base, df_disaster)
        return df_final

    def process_base_2(self, city_files, disaster_file, base_name):
        """Processamento da base 2 (apenas diário)."""
        file_path = self.silver_path / base_name / city_files[0]
        df = utils.read_data_from_parquet(file_path)
        df['season'] = df['date'].apply(utils.get_season)
        df_disaster = utils.read_data_from_parquet(disaster_file).drop(columns=['location'], errors='ignore')
        df_final = self.merge_with_disasters(df, df_disaster)
        return df_final

    def process_base_3(self, city_files, disaster_file, base_name):
        """Processamento da base 3 (diário + horário com preenchimento)."""
        df_daily, df_hourly = None, None

        for file in city_files:
            file_path = self.silver_path / base_name / file
            if "daily" in file:
                df_daily = utils.read_data_from_parquet(file_path)
            elif "hourly" in file:
                df_hourly = utils.read_data_from_parquet(file_path)

        df_disaster = utils.read_data_from_parquet(disaster_file).drop(columns=['location'], errors='ignore')

        # Agregações de dados horários
        medias_por_dia = df_hourly.groupby('date').mean(numeric_only=True).reset_index()
        medias_por_dia = medias_por_dia[['date', 'dewpoint', 'relative_humidity', 'wind_direction', 'wind_speed', 'precipitation']]

        merge_cols = ['precipitation', 'wind_direction', 'wind_speed']
        df_merged = pd.merge(df_daily, medias_por_dia, on=merge_cols, how='left', suffixes=('_daily', '_agg'))

        for col in merge_cols:
            df_daily[col] = df_daily[col].fillna(df_merged[col])

        df_daily.drop(columns=['total_sunshine_duration', 'wind_gust'], errors='ignore', inplace=True)
        medias_por_dia.drop(columns=merge_cols, inplace=True, errors='ignore')

        df_base = pd.merge(df_daily, medias_por_dia, on='date', how='left')
        df_base = self.interpolate_missing_values(df_base)

        df_base['season'] = df_base['date'].apply(utils.get_season)
        df_final = self.merge_with_disasters(df_base, df_disaster)
        return df_final

    def merge_with_disasters(self, df: pd.DataFrame, df_disaster: pd.DataFrame) -> pd.DataFrame:
        """Une base com dataset de desastres e define coluna 'disaster_occurred'."""
        result_df = pd.merge(df, df_disaster, on='date', how='outer')
        result_df['disaster_occurred'] = result_df['eventType'].apply(lambda x: 0 if pd.isna(x) else 1)
        return result_df

    def interpolate_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Interpola valores com até 40% de nulos."""
        null_pct = utils.count_null_values(df)
        cols = null_pct[(null_pct > 0) & (null_pct <= 40)].index
        for col in cols:
            df[col] = df[col].interpolate()
        return df

    def get_processor(self, base_name: str):
        return {
            'base_1': self.process_base_1,
            'base_2': self.process_base_2,
            'base_3': self.process_base_3,
        }.get(base_name, lambda *args: pd.DataFrame())
    def _analyze_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Analisa e retorna um DataFrame com porcentagem de valores nulos por coluna."""
        null_counts = df.isnull().sum()
        total_rows = len(df)
        null_pct = (null_counts / total_rows) * 100
        return null_pct[null_pct > 0].sort_values(ascending=False)

    def process_all(self):
        """Executa o processamento completo para todas as bases e cidades."""
        logger.info("Iniciando processamento Gold Layer...")

        for base_name in os.listdir(self.silver_path):
            base_path = self.silver_path / base_name
            if not base_path.is_dir() or base_name == "base_disaster":
                continue

            logger.info(f"\nBase: {base_name}")
            city_files_map = defaultdict(list)

            for file in os.listdir(base_path):
                for city in self.cities:
                    if city.lower() in file.lower():
                        city_files_map[city].append(file)

            processor = self.get_processor(base_name)

            for city, city_files in city_files_map.items():
                logger.info(f"\nCidade: {city}")
                disaster_file = self.get_disaster_file_for_city(city)
                if disaster_file is None:
                    logger.warning(f" Desastre não encontrado para {city}. Pulando.")
                    continue

                df_final = processor(city_files, disaster_file, base_name)
                if df_final is not None and not df_final.empty:
                    out_path = Path(self.gold_path) / base_name
                    out_path.mkdir(parents=True, exist_ok=True)
                    file_name = out_path / f"{city}_1973_2024.parquet"

                    logger.info(f"Salvando: {file_name}")
                    utils.save_data_to_parquet(df_final, file_name)
                else:
                    logger.warning(f" Dados de saída vazios para {city} ({base_name})")

        logger.info(" Gold Layer concluída.")
def main():
    silver_path = "data/silver"
    gold_path = "data/gold"

    processor = GoldProcessor(silver_path, gold_path)

    start = time.time()
    processor.process_all()
    end = time.time()   
    logger.info(f"\nTempo total: {end - start:.2f} segundos")
if __name__ == "__main__":
    main()