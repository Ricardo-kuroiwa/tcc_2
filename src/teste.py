import os

import yaml
from dotenv import load_dotenv

# Carrega variáveis do .env
load_dotenv()

# Carrega YAML
with open("configs/config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Exemplo de uso
raw_path = config["paths"]["raw_data"]
mlflow_uri = os.getenv("MLFLOW_TRACKING_URI", config["mlflow"]["tracking_uri"])
seed = config["training"]["seed"]

print("Raw path:", raw_path)
print("MLflow URI:", mlflow_uri)
print("Seed:", seed)
