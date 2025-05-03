import time
import os
import tempfile
import optuna
import matplotlib.pyplot as plt
import seaborn as sns

import mlflow 
import xgboost as xgb
from sklearn.metrics import (
classification_report, 
roc_auc_score, 
confusion_matrix, 
precision_score, 
recall_score,
f1_score
)
from imblearn.over_sampling import SMOTE, ADASYN
from imblearn.under_sampling import RandomUnderSampler
from imblearn.combine import SMOTEENN



from dotenv import load_dotenv
"""
Balanced Methods:
- SMOTE
- ADASYN
- RandomUnderSampler
- SMOTEENN
"""
load_dotenv()
mlflow_tracking_uri = os.getenv('MLFLOW_TRACKING_URI')
def train_xgboost(X_train, y_train, X_test, y_test,collumn_target,base,city, balancing_method=None):
    metodos_balanceamento = {
        'SMOTE': SMOTE(random_state=42),
        'ADASYN': ADASYN(random_state=42),
        'RandomUnderSampler': RandomUnderSampler(random_state=42),
        'SMOTEENN': SMOTEENN(random_state=42)
    }
    try:
        if balancing_method  in metodos_balanceamento:
            X_train, y_train = metodos_balanceamento[balancing_method].fit_resample(X_train, y_train)

        
        def objective(trial):
            params = {
                'n_estimators': trial.suggest_categorical('n_estimators', [100, 200, 300]),
                'max_depth': trial.suggest_int('max_depth', 3, 10),
                'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3),
                'subsample': trial.suggest_float('subsample', 0.3, 1.0),
                'colsample_bytree': trial.suggest_float('colsample_bytree', 0.5, 1.0),
                'use_label_encoder': False,
                'eval_metric': 'logloss'
            }

            model = xgb.XGBClassifier(**params, random_state=42)
            model.fit(X_train, y_train)
            preds = model.predict(X_test)
            return recall_score(y_test, preds)
        experiment_name = f"DataBase_{base}"
        mlflow.set_experiment(experiment_name)
        start_time = time.time()
        with mlflow.start_run(run_name=f"XGBoost Classifier Optuna - {city}"):
            study = optuna.create_study(direction='maximize')
            study.optimize(objective, n_trials=50)

            # Melhor modelo
            best_params = study.best_params
            best_model = xgb.XGBClassifier(**best_params, use_label_encoder=False, eval_metric='logloss', random_state=42)
            best_model.fit(X_train, y_train)

            # Previsões e métricas
            y_pred = best_model.predict(X_test)
            auc_roc = roc_auc_score(y_test, best_model.predict_proba(X_test)[:, 1])
            precision = precision_score(y_test, y_pred)
            recall = recall_score(y_test, y_pred)
            f1 = f1_score(y_test, y_pred)

            # Log no MLflow
            mlflow.log_param("balancing_method", balancing_method if balancing_method else "None")
            mlflow.log_params(best_params)
            mlflow.log_param("collumn_target", collumn_target)
            mlflow.log_param("base", base)
            mlflow.log_param("city", city)
            mlflow.log_metric("training_time", time.time() - start_time)
            mlflow.log_metric("precision", precision)
            mlflow.log_metric("recall", recall)
            mlflow.log_metric("f1_score", f1)
            mlflow.log_metric("auc_roc", auc_roc)

            # Log do modelo
            mlflow.sklearn.log_model(best_model, "xgboost_model")

            # Matriz de confusão
            cm = confusion_matrix(y_test, y_pred)
            plt.figure(figsize=(6, 4))
            sns.heatmap(cm, annot=True, fmt='d', cmap='Blues',
                        xticklabels=['0', '1'],
                        yticklabels=['0', '1'])
            plt.xlabel('Predito')
            plt.ylabel('Real')
            plt.title('Matriz de Confusão')
            with tempfile.TemporaryDirectory() as temp_dir:
                timestamp = int(time.time())
                cm_path = os.path.join(temp_dir, f"confusion_matrix_{timestamp}.png")
                plt.savefig(cm_path)
                mlflow.log_artifact(cm_path, artifact_path="plots")
            plt.close()

    except Exception as e:
        print(f"Erro ao treinar o modelo: {e}")
