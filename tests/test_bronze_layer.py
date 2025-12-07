import sys
from pathlib import Path
# Ajuste o sys.path ANTES de qualquer outro import
sys.path.append(str(Path(__file__).parent.parent / "src"))

import pytest
import pandas as pd
import json
from src.data_transform.bronze_processor import BronzeProcessor

@pytest.fixture
def sample_csv(tmp_path):
    # Cria um CSV de exemplo
    csv_path = tmp_path / "city1.csv"
    df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    df.to_csv(csv_path, index=False)
    return csv_path

@pytest.fixture
def sample_mapping(tmp_path):
    # Cria um arquivo de mapeamento de exemplo
    mapping = {"city1": {"col1": "col1", "col2": "col2"}}
    mapping_path = tmp_path / "mapping.json"
    with open(mapping_path, "w") as f:
        json.dump(mapping, f)
    return mapping_path

def test_load_mapping(sample_mapping):
    processor = BronzeProcessor("dummy", "dummy", sample_mapping)
    assert "city1" in processor.mapping

def test_read_csv_file(sample_csv, sample_mapping, tmp_path):
    processor = BronzeProcessor(tmp_path, tmp_path, sample_mapping)
    df = processor.read_csv_file(sample_csv)
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (2, 2)

def test_add_metadata(sample_csv, sample_mapping, tmp_path):
    processor = BronzeProcessor(tmp_path, tmp_path, sample_mapping)
    df = pd.read_csv(sample_csv)
    source_info = {"file_name": "city1.csv", "base_name": "base1", "city_name": "city1"}
    df_meta = processor.add_metadata(df, source_info)
    assert "_bronze_ingestion_timestamp" in df_meta.columns
    assert "_source_file" in df_meta.columns

def test_save_to_parquet(sample_csv, sample_mapping, tmp_path):
    processor = BronzeProcessor(tmp_path, tmp_path, sample_mapping)
    df = pd.read_csv(sample_csv)
    output_path = tmp_path / "test.parquet"
    result = processor.save_to_parquet(df, output_path)
    assert result is True
    assert output_path.exists()

def test_process_single_file(sample_csv, sample_mapping, tmp_path):
    # Simula estrutura de base/cidade
    base_dir = tmp_path / "base1"
    base_dir.mkdir()
    csv_path = base_dir / "city1.csv"
    pd.DataFrame({"col1": [1], "col2": ["a"]}).to_csv(csv_path, index=False)
    processor = BronzeProcessor(tmp_path, tmp_path, sample_mapping)
    result = processor.process_single_file(csv_path, "base1")
    assert result is True
    parquet_path = tmp_path / "base1" / "city1.parquet"
    assert parquet_path.exists()