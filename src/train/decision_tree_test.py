import optuna 
import optuna.visualization.matplotlib as optuna_viz

import os
import time
import json
from dotenv import load_dotenv
import tempfile
import matplotlib.pyplot as plt
import seaborn as sns
import mlflow 
import mlflow.sklearn

from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import RandomizedSearchCV
from sklearn.metrics import (
    classification_report, 
    roc_auc_score,
    confusion_matrix,
    precision_score,
    recall_score, 
    f1_score
)
from sklearn.feature_selection import SelectKBest, RFE, SelectFromModel

from imblearn.over_sampling import SMOTE, ADASYN
from imblearn.under_sampling import RandomUnderSampler
from imblearn.combine import SMOTEENN

from src.utils.balancing_conf import balancing_methods
load_dotenv()
mlflow_tracking_uri = os.getenv('MLFLOW_TRACKING_URI')
"""
Balanced Methods:
- SMOTE
- ADASYN
- RandomUnderSampler
- SMOTEENN
Selection feature:
- SelectKBest
- RFE
- SelectFromModel
"""
def train_decision_tree(X_train, y_train, X_test, y_test, collumn_target,base,city,balancing_method=None):
    try:
        
        def objective(trial):
            params = {
                'max_depth': trial.suggest_int('max_depth', 2, 50),
                'min_samples_split': trial.suggest_int('min_samples_split', 2, 10),
                'min_samples_leaf': trial.suggest_int('min_samples_leaf', 1, 10),
                'max_features': trial.suggest_categorical('max_features', [None, 'sqrt', 'log2'])
            }
            balanceador  = None
            if balancing_method is not None:
                balanceador = balancing_methods[balancing_method](trial)
                X_train, y_train = balanceador.fit_resample(X_train, y_train)
            # Feature selection

            model = DecisionTreeClassifier(**params, random_state=42)
            model.fit(X_train, y_train)
            pred = model.predict(X_test)
            return recall_score(y_test, pred)
        
        start_time = time.time()
        experiment_name = f"DataBase_{base}"
        mlflow.set_experiment(experiment_name)
        with mlflow.start_run(run_name=f"Decision Tree Classifier Optuna - {city}"):
            study = optuna.create_study(direction='maximize')
            study.optimize(objective, n_trials=50)
            best_params = study.best_params

             # Obtém o balanceador otimizado e re-aplica no conjunto de treino
            balanceador = balancing_methods[balancing_method](study.best_trial) if balancing_method else None
            X_train, y_train = balanceador.fit_resample(X_train, y_train) if balanceador else (X_train, y_train)

            best_model = DecisionTreeClassifier(**best_params, random_state=42)
            best_model.fit(X_train, y_train)
            y_pred = best_model.predict(X_test)

            auc_roc = roc_auc_score(y_test, best_model.predict_proba(X_test)[:, 1])
            precision = precision_score(y_test, y_pred)
            recall = recall_score(y_test, y_pred)
            f1 = f1_score(y_test, y_pred)

            # Logging no MLflow
            mlflow.log_param("balancing_method", balancing_method if balancing_method else "None")

            
            mlflow.log_params(best_params)
            mlflow.log_param("feature_selection", "None")
            mlflow.log_param("size", len(best_params))
            mlflow.log_param("collumn_target", collumn_target)
            mlflow.log_param("base", base)
            mlflow.log_param("city", city)
            mlflow.log_metric("training_time", time.time() - start_time)
            mlflow.log_metric("precision", precision)
            mlflow.log_metric("recall", recall)
            mlflow.log_metric("f1_score", f1)
            mlflow.log_metric("auc_roc", auc_roc)

            mlflow.sklearn.log_model(best_model, "decision_tree_model")
            cm = confusion_matrix(y_test, y_pred)
            plt.figure(figsize=(6, 4))
            sns.heatmap(cm, annot=True, fmt='d', cmap='Blues',
                        xticklabels=['0', '1'],
                        yticklabels=['0', '1'])
            plt.xlabel('Predito')
            plt.ylabel('Real')
            plt.title('Matriz de Confusão')
            # Salvar imagem temporária e logar no MLflow
            with tempfile.TemporaryDirectory() as temp_dir:
                cm_path = os.path.join(temp_dir, "confusion_matrix.png")
                plt.savefig(cm_path)
                mlflow.log_artifact(cm_path, artifact_path="plots")
            # === Optimization History Plot ===
            fig_hist = optuna_viz.plot_optimization_history(study)
            fig_hist.figure.set_size_inches(8, 5)  # ajustar o tamanho opcionalmente

            # === Hyperparameter Importance Plot ===
            fig_importance = optuna_viz.plot_param_importances(study)
            fig_importance.figure.set_size_inches(8, 5)

            # Salvar os plots como arquivos temporários
            with tempfile.TemporaryDirectory() as temp_dir:
                hist_path = os.path.join(temp_dir, "optimization_history.png")
                imp_path = os.path.join(temp_dir, "hyperparameter_importance.png")

                fig_hist.figure.savefig(hist_path)
                fig_importance.figure.savefig(imp_path)

                mlflow.log_artifact(hist_path, artifact_path="optuna_plots")
                mlflow.log_artifact(imp_path, artifact_path="optuna_plots")

            # Fechar figuras para liberar memória
            plt.close(fig_hist.figure)
            plt.close(fig_importance.figure)
            plt.close()
            


    except Exception as e:
        print(f"Erro ao treinar o modelo: {e}")
