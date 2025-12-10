import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

from src.utils.logger import setup_logger

logger = setup_logger("bronze_processor")


class BronzeProcessor:
    """
    Camada Bronze: Responsável apenas por ingestão e conversão de formato.
    """

    def __init__(
        self, raw_data_path: str, bronze_output_path: str, mapping_file_path: str
    ):
        """
        Inicializa o Bronze Processor com caminhos de entrada e saída.
        Parameters:
            raw_data_path (str): Caminho para os dados brutos (CSV).
            bronze_output_path (str): Caminho para salvar os dados processados em Parquet.
            mapping_file_path (str): Caminho para o arquivo de mapeamento de colunas.
        """
        self.raw_data_path = Path(raw_data_path)
        self.bronze_output_path = Path(bronze_output_path)
        self.mapping = self._load_mapping(mapping_file_path)

        # Criar diretório de saída
        self.bronze_output_path.mkdir(parents=True, exist_ok=True)

        logger.info(f"Bronze Processor inicializado")
        logger.info(f"Input path: {self.raw_data_path}")
        logger.info(f"Output path: {self.bronze_output_path}")

    def _load_mapping(self, file_path: str) -> Dict:
        """
        Carrega mapeamento de colunas de um arquivo JSON.
        Parameters:
            file_path (str): Caminho para o arquivo de mapeamento.
        Return :
            Dict: Mapeamento de colunas carregado.
        """
        try:
            with open(file_path, "r") as f:
                mapping = json.load(f)
                logger.debug(f"Mapeamento carregado com {len(mapping)} bases")
                return mapping
        except Exception as e:
            logger.error(f"Erro ao carregar mapeamento: {e}", exc_info=True)
            raise

    def read_csv_file(self, file_path: Path) -> Optional[pd.DataFrame]:
        """
        Lê um arquivo CSV e retorna um DataFrame
        Parameters:
            file_path (Path): Caminho para o arquivo CSV.
        Return:
            Optional[pd.DataFrame]: DataFrame contendo os dados do CSV ou None em caso de
        """
        try:
            df = pd.read_csv(
                file_path,
                on_bad_lines="skip",
                low_memory=False,  # Evita warnings de tipos mistos
            )

            logger.info(f"Arquivo lido: {file_path.name} | Shape: {df.shape}")
            return df

        except Exception as e:
            logger.error(f"Erro ao ler {file_path}: {e}", exc_info=True)
            return None

    def add_metadata(self, df: pd.DataFrame, source_info: Dict) -> pd.DataFrame:
        """
        Adiciona metadados importantes para rastreabilidade.
        Parameters:
            df (pd.DataFrame): DataFrame a ser modificado.
            source_info (Dict): Informações sobre a origem do arquivo.
        Return:
            pd.DataFrame: DataFrame com colunas de metadados adicionadas.
        """
        df["_bronze_ingestion_timestamp"] = datetime.now()
        df["_source_file"] = source_info["file_name"]
        df["_source_base"] = source_info["base_name"]
        df["_source_city"] = source_info["city_name"]
        df["_bronze_version"] = "1.0"

        return df

    def save_to_parquet(self, df: pd.DataFrame, output_path: Path) -> bool:
        """
        Salva DataFrame em Parquet.
        Parameters:
            df (pd.DataFrame): DataFrame a ser salvo.
            output_path (Path): Caminho de saída para o arquivo Parquet.
        Return:
            bool: True se o salvamento foi bem-sucedido, False caso contrário.
        """
        try:
            df.to_parquet(
                output_path,
                index=False,
                engine="pyarrow",
                compression="snappy",  # Melhor balanço velocidade/compressão
            )

            file_size_mb = output_path.stat().st_size / (1024 * 1024)
            logger.info(
                f"Arquivo salvo: {output_path.name} | Size: {file_size_mb:.2f} MB"
            )
            return True

        except Exception as e:
            logger.error(f"Erro ao salvar {output_path}: {e}", exc_info=True)
            return False

    def process_single_file(self, file_path: Path, base_name: str) -> bool:
        """
        Processa um único arquivo CSV.
        Parameters:
            file_path (Path): Caminho para o arquivo CSV.
            base_name (str): Nome da base (subpasta) onde o arquivo está localizado.
        Return:
            bool: True se o processamento foi bem-sucedido, False caso contrário.
        """
        # Extrair nome da cidade do arquivo
        city_name = file_path.stem  # Ex: "cidade1.csv" -> "cidade1"

        logger.info(f"Processando: {base_name}/{city_name}")

        # Ler arquivo
        df = self.read_csv_file(file_path)
        if df is None:
            return False

        # Adicionar metadados
        source_info = {
            "file_name": file_path.name,
            "base_name": base_name,
            "city_name": city_name,
        }
        df = self.add_metadata(df, source_info)

        # Definir caminho de saída mantendo estrutura base/cidade
        output_dir = self.bronze_output_path / base_name
        output_dir.mkdir(exist_ok=True)
        output_path = output_dir / f"{city_name}.parquet"

        # Salvar
        return self.save_to_parquet(df, output_path)

    def process_all_bases(self) -> Dict[str, int]:
        """
        Processa todas as bases e retorna estatísticas.
        Return:
            Dict[str, int]: Estatísticas de processamento, total de arquivos
        """
        stats = {"total_files": 0, "successful": 0, "failed": 0, "bases_processed": []}

        # Iterar sobre cada base (subpasta)
        for base_folder in self.raw_data_path.iterdir():
            if not base_folder.is_dir():
                continue

            base_name = base_folder.name
            logger.info(f"\n{'='*50}")
            logger.info(f"Processando base: {base_name}")
            logger.info(f"{'='*50}")

            base_file_count = 0

            # Processar cada arquivo CSV da base
            for csv_file in sorted(base_folder.glob("*.csv")):
                stats["total_files"] += 1

                if self.process_single_file(csv_file, base_name):
                    stats["successful"] += 1
                    base_file_count += 1
                else:
                    stats["failed"] += 1

            if base_file_count > 0:
                stats["bases_processed"].append(base_name)
                logger.info(f"Base {base_name}: {base_file_count} arquivos processados")

        return stats

    def validate_bronze_output(self) -> Dict[str, int]:
        """
        Valida a saída da camada Bronze.
        """
        logger.info("\nValidando output Bronze...")

        validation = {}

        for base_folder in self.bronze_output_path.iterdir():
            if base_folder.is_dir():
                parquet_files = list(base_folder.glob("*.parquet"))
                validation[base_folder.name] = len(parquet_files)

                # Verificar integridade básica
                for pq_file in parquet_files[:1]:  # Testar apenas o primeiro
                    try:
                        df = pd.read_parquet(pq_file)
                        logger.debug(
                            f"Validação OK: {pq_file.name} - Shape: {df.shape}"
                        )
                    except Exception as e:
                        logger.error(f"Erro na validação de {pq_file}: {e}")

        return validation


def main():
    """Função principal para executar o Bronze Processor."""

    # Configurações
    config = {
        "raw_data_path": "data/raw",
        "bronze_output_path": "data/bronze",
        "mapping_file": "configs/dataframe_column_mapping.json",
    }

    logger.info("Iniciando Bronze Processing...")
    start_time = time.time()

    try:
        # Inicializar processor
        processor = BronzeProcessor(
            raw_data_path=config["raw_data_path"],
            bronze_output_path=config["bronze_output_path"],
            mapping_file_path=config["mapping_file"],
        )

        # Processar todas as bases
        stats = processor.process_all_bases()

        # Validar output
        validation = processor.validate_bronze_output()

        # Relatório final
        elapsed_time = time.time() - start_time

        logger.info("\n" + "=" * 60)
        logger.info("BRONZE PROCESSING COMPLETO")
        logger.info("=" * 60)
        logger.info(f"Tempo total: {elapsed_time:.2f} segundos")
        logger.info(f"Total de arquivos: {stats['total_files']}")
        logger.info(f"Sucesso: {stats['successful']}")
        logger.info(f"Falhas: {stats['failed']}")
        logger.info(f"Bases processadas: {', '.join(stats['bases_processed'])}")
        logger.info("\nArquivos por base:")
        for base, count in validation.items():
            logger.info(f"  - {base}: {count} arquivos")

    except Exception as e:
        logger.error(f"Erro fatal no Bronze Processing: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
