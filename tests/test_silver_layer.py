import pytest
import pandas as pd
import json
from pathlib import Path
from unittest.mock import patch
from src.data_transform.silver_processor import SilverProcessor

@pytest.fixture
def sample_mapping(tmp_path):
    mapping = {"base_test": {"old_col": "new_col"}}
    mapping_path = tmp_path / "mapping.json"
    mapping_path.write_text(json.dumps(mapping))
    return str(mapping_path)

@pytest.fixture
def processor(tmp_path, sample_mapping):
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    bronze.mkdir()
    silver.mkdir()
    return SilverProcessor(str(bronze), str(silver), sample_mapping)
def test_rename_columns(processor):
    df = pd.DataFrame({"old_col": [1, 2, 3]})
    result = processor.rename_columns(df, "base_test")
    assert "new_col" in result.columns
    assert "old_col" not in result.columns
def test_convert_to_datetime(processor):
    df = pd.DataFrame({"date_col": ["2024-01-01", "2024-01-02"]})
    df = processor.convert_to_datetime(df)
    assert pd.api.types.is_datetime64_any_dtype(df["date_col"])
def test_handle_nas_interpolate(processor):
    df = pd.DataFrame({"a": [1, None, 3]})
    result = processor.handle_nas(df)
    assert result["a"].isnull().sum() == 0
def test_detect_city_and_type(processor):
    city, tipo = processor.detect_city_and_type("houston_daily_data.parquet")
    assert city == "houston"
    assert tipo == "daily"
@patch("pandas.read_parquet")
def test_read_parquet(mock_read, processor):
    mock_read.return_value = pd.DataFrame({"a": [1]})
    df = processor.read_parquet(Path("fake.parquet"))
    assert not df.empty
def test_process_file(processor):
    df = pd.DataFrame({
        "old_col": [1, 2, 3],
        "date_col": ["2024-01-01", "2024-01-02", "2024-01-03"]
    })
    result = processor.process_file(df, "base_test")
    assert "new_col" in result.columns
    assert pd.api.types.is_datetime64_any_dtype(result["date_col"])