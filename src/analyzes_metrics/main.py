import mlflow
from mlflow.tracking import MlflowClient
import pandas as pd

# Inicializa o cliente
client = MlflowClient()

# Define o nome ou ID do experimento (substitua pelo seu)
experiment_name = "DataBase_base_1"  # ou use o ID direto
experiment = client.get_experiment_by_name(experiment_name)

if experiment is None:
    raise Exception(f"Experimento '{experiment_name}' não encontrado!")

experiment_id = experiment.experiment_id

# Busca todas as execuções (runs) do experimento
runs = client.search_runs(experiment_ids=[experiment_id])

# Extrai dados em formato de DataFrame
runs_data = []
for run in runs:
    data = {
        "run_id": run.info.run_id,
        "status": run.info.status,
        "start_time": run.info.start_time,
        "end_time": run.info.end_time
    }
    data.update(run.data.params)
    data.update(run.data.metrics)
    runs_data.append(data)
    
    print(f"keys : {run.data.metrics.keys()}")

# Converte para DataFrame
df = pd.DataFrame(runs_data)

# Mostra os dados
print(df)
