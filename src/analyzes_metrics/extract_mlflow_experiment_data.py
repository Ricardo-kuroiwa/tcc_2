import json

import mlflow
import pandas as pd
from mlflow.tracking import MlflowClient

import src.utils.Utils as utils


def get_data_experiment(experiment_name: str) -> pd.DataFrame:
    """
    Função para buscar dados de um experimento específico no MLflow.
    Parans:
        - experiment_name: Nome do experimento no MLflow.
    Return:
        - Frame com os dados do experimento.
    """
    # Init client
    client = MlflowClient()
    experiment = client.get_experiment_by_name(experiment_name)
    # Check if experiment exists
    if experiment is None:
        raise Exception(f"Experimento '{experiment_name}' não encontrado!")

    experiment_id = experiment.experiment_id
    # Search for runs in the experiment
    runs = client.search_runs(experiment_ids=[experiment_id])
    runs_data = []
    for run in runs:
        data = {
            "run_id": run.info.run_id,
            "status": run.info.status,
            "run_name": run.data.tags.get("mlflow.runName", "Unnamed Run"),
            "model_name": json.loads(run.data.tags.get("mlflow.log-model.history"))[0][
                "artifact_path"
            ],
        }
        data.update(run.data.params)
        data.update(run.data.metrics)
        runs_data.append(data)
    df = pd.DataFrame(runs_data)
    return df


if __name__ == "__main__":
    # Get data from experiment
    names_experiments = ["DataBase_base_1", "DataBase_base_2", "DataBase_base_3"]
    for name in names_experiments:
        df = get_data_experiment(name)
        print(f"Dados do experimento '{name}':")
        print(df.head(5))
        # Save data to parquet
        path = f"src/analyzes_metrics/data/{name}.parquet"
        utils.save_data_to_parquet(df, path)
        print(f"Dados salvos em {path}")
