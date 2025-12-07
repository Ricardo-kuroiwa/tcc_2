import os
import pandas as pd
from pathlib import Path
from typing import Dict
import json
import time

from src.utils.logger import setup_logger

logger = setup_logger("silver_processor")


class SilverProcessor:
    def __init__(self, bronze_path: str, silver_path: str, mapping_file_path: str):
        self.bronze_path = Path(bronze_path)
        self.silver_path = Path(silver_path)
        self.mapping = self._load_mapping(mapping_file_path)

        self.silver_path.mkdir(parents=True, exist_ok=True)
        logger.info("Silver Processor inicializado")
        logger.info(f"Input path: {self.bronze_path}")
        logger.info(f"Output path: {self.silver_path}")

    def _load_mapping(self, path: str) -> Dict:
        try:
            with open(path, 'r') as f:
                mapping = json.load(f)
                logger.debug(f"Mapeamento carregado: {list(mapping.keys())}")
                return mapping
        except Exception as e:
            logger.error(f"Erro ao carregar mapeamento: {e}")
            raise

    def read_parquet(self, path: Path) -> pd.DataFrame:
        try:
            df = pd.read_parquet(path)
            logger.debug(f"Lido arquivo: {path.name} - shape {df.shape}")
            return df
        except Exception as e:
            logger.error(f"Erro ao ler {path}: {e}")
            return pd.DataFrame()

    def rename_columns(self, df: pd.DataFrame, base_name: str) -> pd.DataFrame:
        if base_name in self.mapping:
            rename_map = self.mapping[base_name]
            df = df.rename(columns=rename_map)
            logger.info(f"Colunas renomeadas: {rename_map}")
        return df

    def convert_to_datetime(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in df.columns:
            if 'date' in col.lower():
                try:
                    #df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
                    if pd.api.types.is_numeric_dtype(df[col]):
                        df[col] = pd.to_datetime(df[col], unit='ms', errors='coerce',utc=True)
                    else:
                        df[col] = pd.to_datetime(df[col], errors='coerce',utc=True)
                    df[col] = df[col].dt.tz_localize(None)
                    logger.debug(f"Coluna convertida para datetime: {col} | Tipo final: {df[col].dtype}")
                except Exception as e:
                    logger.warning(f"Falha ao converter {col}: {e}")
            logger.debug(f"Entrou na conversão de data para {col} com tipo {df[col].dtype}")
        return df

    def handle_nas(self, df: pd.DataFrame) -> pd.DataFrame:
        na_pct = df.isnull().mean() * 100
        for col, pct in na_pct.items():
            if pct == 0:
                continue
            elif pct <= 20:
                df[col] = df[col].interpolate(method="linear", limit_direction="both")
                logger.info(f"Interpolado: {col}")
            elif pct <= 50:
                df[col] = df[col].fillna(-999)
                logger.info(f"Nulo preenchido com -999: {col}")
            else:
                logger.warning(f"Muitos nulos em {col} ({pct:.1f}%) — mantido como está")
        return df

    def detect_city_and_type(self, name: str) -> (str, str): # type: ignore
        name = name.lower()
        tipo = "daily" if "daily" in name else "hourly"
        for c in ['houston', 'chicago', 'dallas', 'nashville', 'miami',
                  'new york', 'oklahoma city', 'albuquerque']:
            if c.replace(" ", "") in name.replace(" ", ""):
                return c.replace(" ", "_"), tipo
        return "", tipo

    def process_file(self, df: pd.DataFrame, base_name: str) -> pd.DataFrame:
        logger.info(f"Processando arquivo - base: {base_name} - linhas: {df.shape[0]}")

        df = self.rename_columns(df, base_name)
        df = self.convert_to_datetime(df)
        df = self.handle_nas(df)

        # Agregação: se for horários, converte para diário
        if 'date' in df.columns and pd.api.types.is_datetime64_any_dtype(df['date']):
            if df['date'].dt.hour.nunique() > 1:
                logger.info("Agregando dados horários → diário")
                df['date'] = df['date'].dt.date
                num_cols = df.select_dtypes(include='number').columns.tolist()
                df = df.groupby('date')[num_cols].mean().reset_index()
                df['date'] = pd.to_datetime(df['date'])
                logger.debug(f"Numero de dias duplicados : {df['date'].duplicated().sum()}")

        return df

    def save_parquet(self, df: pd.DataFrame, out_path: Path):
        try:
            out_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(out_path, index=False)
            logger.info(f"Salvo: {out_path.name} - shape: {df.shape}")
        except Exception as e:
            logger.error(f"Erro ao salvar {out_path}: {e}")

    def process_all_by_city(self):
        """Processa arquivos da Bronze separadamente por cidade e tipo, e salva individualmente na Silver."""
        logger.info("Iniciando processamento Silver por cidade")

        for base_folder in sorted(self.bronze_path.iterdir()):
            if not base_folder.is_dir():
                continue

            base_name = base_folder.name
            silver_base_path = self.silver_path / base_name
            silver_base_path.mkdir(parents=True, exist_ok=True)

            logger.info(f"\nBase: {base_name}")

            for parquet_file in sorted(base_folder.glob("*.parquet")):
                df = self.read_parquet(parquet_file)
                if df.empty:
                    logger.warning(f"Arquivo vazio ou inválido: {parquet_file.name}")
                    continue

                city, tipo = self.detect_city_and_type(parquet_file.name)
                if not city:
                    logger.warning(f"Cidade não identificada no nome do arquivo: {parquet_file.name}")
                    continue

                logger.info(f"Cidade detectada: {city} | Tipo: {tipo}")
                processed_df = self.process_file(df, base_name)

                if processed_df.empty:
                    logger.warning(f"Arquivo processado resultou vazio: {parquet_file.name}")
                    continue

                output_file = silver_base_path / f"{city}_{tipo}.parquet"
                self.save_parquet(processed_df, output_file)

        logger.info("\nProcessamento por cidade finalizado!")
    def process_base_disaster(self):
        """
        Processa os arquivos da base_disaster e salva os desastres por cidade.
        """
        logger.info("\nProcessando base_disaster por cidade")

        base_disaster_path = self.bronze_path / "base_disaster"
        silver_output_path = self.silver_path / "base_disaster"
        silver_output_path.mkdir(parents=True, exist_ok=True)

        if not base_disaster_path.exists():
            logger.warning("Pasta base_disaster não encontrada.")
            return

        for file in sorted(base_disaster_path.glob("*.parquet")):
            df = self.read_parquet(file)
            if df.empty:
                logger.warning(f"Arquivo vazio ou inválido: {file.name}")
                continue

            # Detecta cidade a partir do nome do arquivo
            city, _ = self.detect_city_and_type(file.name)
            if not city:
                logger.warning(f"Cidade não detectada em: {file.name}")
                continue

            logger.info(f"Cidade detectada: {city}")

            # Renomear colunas se necessário
            base_name = "base_disaster"
            df = self.rename_columns(df, base_name)
            df = self.convert_to_datetime(df)
            
            # Filtrar as colunas principais (exemplo: date, eventType, location etc.)
            relevant_cols = ['date', 'eventType']
            df = df[[col for col in relevant_cols if col in df.columns]]
            logger.info(f"Colnunas {df.columns} renomeadas para {base_name  }")
            output_file = silver_output_path / f"{city}.parquet"
            self.save_parquet(df, output_file)

        logger.info("base_disaster processada com sucesso.")
    def validate_silver_output_by_city(self):
        """
        Gera relatório de qualidade por cidade/arquivo.
        """
        logger.info("\nRELATÓRIO DE QUALIDADE - SILVER (POR CIDADE)")
        logger.info("=" * 60)

        for base_folder in self.silver_path.glob("base_*"):
            if not base_folder.is_dir():
                continue

            logger.info(f"\n Base: {base_folder.name}")

            for file in base_folder.glob("*.parquet"):
                try:
                    df = pd.read_parquet(file)
                    base_name = base_folder.name
                    city_name = file.stem.replace(".parquet", "")

                    logger.info(f"\n {base_name}/{file.name}")
                    logger.info(f"  Shape: {df.shape}")
                    logger.info(f"  Colunas: {list(df.columns)}")
                    
                    nulls = df.isna().sum() / len(df) * 100
                    nulls = nulls[nulls > 0].round(1)

                    if nulls.empty:
                        logger.info("   Nenhum valor nulo.")
                    else:
                        logger.info("   Valores nulos:")
                        for col, pct in nulls.items(): 
                            logger.info(f"    - {col}: {pct:.1f}%")

                except Exception as e:
                    logger.error(f"Erro ao validar {file}: {e}")
def main():
    processor = SilverProcessor(
        bronze_path="data/bronze",
        silver_path="data/silver",
        mapping_file_path="configs/dataframe_column_mapping.json"
    )
    start = time.time()  
    processor.process_all_by_city()
    processor.process_base_disaster()
    end = time.time()
    logger.info(f"Tempo total de execução: {end - start:.2f} segundos")
    # processor.validate_silver_output_by_city()

if __name__ == "__main__":
    main()