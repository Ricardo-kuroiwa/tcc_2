import os
import tempfile
import time

import matplotlib
import matplotlib.pyplot as plt
import optuna
import optuna.visualization.matplotlib as optuna_viz
import seaborn as sns
from dotenv import load_dotenv

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import mlflow
import mlflow.sklearn
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.tree import DecisionTreeClassifier

import src.utils.Utils as utils
from src.utils.balancing_conf import balancing_creators, balancing_methods
from src.utils.feature_selection_conf import (
    feature_selection_creators,
    feature_selection_methods,
)

load_dotenv()
mlflow_tracking_uri = os.getenv("MLFLOW_TRACKING_URI")
"""
Balanced Methods:
- SMOTE
- ADASYN
- RandomUnderSampler
- SMOTEENN
"""


def get_input_example(X_train):
    import pandas as pd

    if isinstance(X_train, pd.DataFrame):
        return X_train.head(1)
    else:
        return X_train[0:1]


def train_decision_tree(
    X_train,
    y_train,
    X_test,
    y_test,
    collumn_target,
    base,
    city,
    balancing_method=None,
    feature_selection_method=None,
    feature_selection_order="after",
    n_trials=50,
    fase=2,
):
    try:

        def objective(trial):
            max_features = X_train.shape[1]
            params = {
                "max_depth": trial.suggest_int("max_depth", 2, 50),
                "min_samples_split": trial.suggest_int("min_samples_split", 2, 10),
                "min_samples_leaf": trial.suggest_int("min_samples_leaf", 1, 10),
                "max_features": trial.suggest_categorical(
                    "max_features", [None, "sqrt", "log2"]
                ),
            }
            X_train_copy, y_train_copy, X_test_copy = (
                X_train.copy(),
                y_train.copy(),
                X_test.copy(),
            )
            balanceador = None
            feature_selector = None
            # Apply feature selection and balancing methods if specified
            if feature_selection_method is not None and balancing_method is not None:
                if feature_selection_order == "before":
                    # First apply feature selection, then balancing
                    feature_selector = feature_selection_methods[
                        feature_selection_method
                    ](trial, max_features)
                    X_train_copy = feature_selector.fit_transform(
                        X_train_copy, y_train_copy
                    )
                    X_test_copy = feature_selector.transform(X_test_copy)
                    balanceador = balancing_methods[balancing_method](trial)
                    X_train_copy, y_train_copy = balanceador.fit_resample(
                        X_train_copy, y_train_copy
                    )
                else:
                    # First apply balancing, then feature selection
                    balanceador = balancing_methods[balancing_method](trial)
                    X_train_copy, y_train_copy = balanceador.fit_resample(
                        X_train_copy, y_train_copy
                    )
                    feature_selector = feature_selection_methods[
                        feature_selection_method
                    ](trial, max_features)
                    X_train_copy = feature_selector.fit_transform(
                        X_train_copy, y_train_copy
                    )
                    X_test_copy = feature_selector.transform(X_test_copy)
            # If only balancing method or feature selection method is specified
            else:
                if balancing_method is not None:
                    balanceador = balancing_methods[balancing_method](trial)
                    X_train_copy, y_train_copy = balanceador.fit_resample(
                        X_train_copy, y_train_copy
                    )
                if feature_selection_method is not None:
                    feature_selector = feature_selection_methods[
                        feature_selection_method
                    ](trial, max_features)
                    X_train_copy = feature_selector.fit_transform(
                        X_train_copy, y_train_copy
                    )
                    X_test_copy = feature_selector.transform(X_test_copy)

            model = DecisionTreeClassifier(**params, random_state=42)
            model.fit(X_train_copy, y_train_copy)
            pred = model.predict(X_test_copy)
            return recall_score(y_test, pred)

        start_time = time.time()
        experiment_name = f"DataBase_{base}_fase_{fase}"
        mlflow.set_experiment(experiment_name)

        with mlflow.start_run(run_name=f"Decision Tree Classifier Optuna - {city}"):
            study = optuna.create_study(direction="maximize")
            study.optimize(objective, n_trials=n_trials)

            best_params = study.best_params
            tree_best_params = {
                k: v
                for k, v in study.best_params.items()
                if k
                in [
                    "max_depth",
                    "min_samples_split",
                    "min_samples_leaf",
                    "max_features",
                ]
            }
            balancer_params = {}
            feature_params = {}
            balancing_method = balancing_method if balancing_method else None
            for k, v in best_params.items():
                if k not in tree_best_params:
                    if balancing_method and k in utils.get_balancing_param_names(
                        balancing_method
                    ):
                        balancer_params[k] = v
                    elif (
                        feature_selection_method
                        and k
                        in utils.get_feature_selection_param_names(
                            feature_selection_method
                        )
                    ):
                        feature_params[k] = v

            if feature_selection_method is not None and balancing_method is not None:
                if feature_selection_order == "before":
                    # First apply feature selection, then balancing
                    feature_selector = feature_selection_creators[
                        feature_selection_method
                    ](**feature_params)
                    X_train = feature_selector.fit_transform(X_train, y_train)
                    X_test = feature_selector.transform(X_test)
                    balancer = balancing_creators[balancing_method](**balancer_params)
                    X_train, y_train = balancer.fit_resample(X_train, y_train)
                else:
                    # First apply balancing, then feature selection
                    balancer = balancing_creators[balancing_method](**balancer_params)
                    X_train, y_train = balancer.fit_resample(X_train, y_train)
                    feature_selector = feature_selection_creators[
                        feature_selection_method
                    ](**feature_params)
                    X_train = feature_selector.fit_transform(X_train, y_train)
                    X_test = feature_selector.transform(X_test)
            else:
                # If only balancing method or feature selection method is specified
                if feature_selection_method is not None:
                    feature_selector = feature_selection_creators[
                        feature_selection_method
                    ](**feature_params)
                    X_train = feature_selector.fit_transform(X_train, y_train)
                    X_test = feature_selector.transform(X_test)
                if balancing_method is not None:
                    balancer = balancing_creators[balancing_method](**balancer_params)
                    X_train, y_train = balancer.fit_resample(X_train, y_train)

            best_model = DecisionTreeClassifier(**tree_best_params, random_state=42)
            best_model.fit(X_train, y_train)
            y_pred = best_model.predict(X_test)

            # Calculate metrics
            auc_roc = roc_auc_score(y_test, best_model.predict_proba(X_test)[:, 1])
            precision = precision_score(y_test, y_pred)
            recall = recall_score(y_test, y_pred)
            f1 = f1_score(y_test, y_pred)

            # Log parameters and metrics
            mlflow.log_param("collumn_target", collumn_target)
            mlflow.log_param("base", base)
            mlflow.log_param("city", city)

            mlflow.log_param(
                "balancing_method", balancing_method if balancing_method else "None"
            )
            mlflow.log_param(
                "feature_selection_method",
                feature_selection_method if feature_selection_method else "None",
            )
            mlflow.log_param("feature_selection_order", feature_selection_order)
            # log best parameters of model
            mlflow.log_params(tree_best_params)
            if balancer_params:
                mlflow.log_params(balancer_params)

            # Log metrics
            mlflow.log_metric("precision", precision)
            mlflow.log_metric("recall", recall)
            mlflow.log_metric("f1_score", f1)
            mlflow.log_metric("auc_roc", auc_roc)
            mlflow.log_metric("training_time", time.time() - start_time)

            # Log model
            mlflow.sklearn.log_model(
                sk_model=best_model,
                artifact_path="decision_tree_model",
                input_example=get_input_example(X_train),
            )

            # Confusion matrix
            cm = confusion_matrix(y_test, y_pred)
            plt.figure(figsize=(6, 4))
            sns.heatmap(
                cm,
                annot=True,
                fmt="d",
                cmap="Blues",
                xticklabels=["0", "1"],
                yticklabels=["0", "1"],
            )
            plt.xlabel("Predito")
            plt.ylabel("Real")
            plt.title("Matriz de Confusão")

            with tempfile.TemporaryDirectory() as temp_dir:
                # Confusion matrix plot
                cm_path = os.path.join(temp_dir, "confusion_matrix.png")
                plt.savefig(cm_path)
                mlflow.log_artifact(cm_path, artifact_path="plots")
                plt.close()

                # Optimization history plot
                fig_hist = optuna_viz.plot_optimization_history(study)
                fig_hist.figure.set_size_inches(8, 5)
                hist_path = os.path.join(temp_dir, "optimization_history.png")
                fig_hist.figure.savefig(hist_path, bbox_inches="tight")
                mlflow.log_artifact(hist_path, artifact_path="plots")

                # Hyperparameter importance plot
                fig_importance = optuna_viz.plot_param_importances(study)
                fig_importance.figure.set_size_inches(8, 5)
                imp_path = os.path.join(temp_dir, "hyperparameter_importance.png")
                fig_importance.figure.savefig(imp_path, bbox_inches="tight")
                mlflow.log_artifact(imp_path, artifact_path="plots")

            plt.close()

    except Exception as e:
        print(f"Erro ao treinar o modelo: {e}")
