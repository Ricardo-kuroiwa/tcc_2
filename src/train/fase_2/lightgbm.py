import os
import time
import tempfile
import mlflow
import mlflow.sklearn
import optuna
import lightgbm as lgb
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
import optuna 
import optuna.visualization.matplotlib as optuna_viz
from sklearn.metrics import (
    roc_auc_score,
    confusion_matrix,
    precision_score,
    recall_score,
    f1_score
)
from imblearn.over_sampling import SMOTE, ADASYN
from imblearn.under_sampling import RandomUnderSampler
from imblearn.combine import SMOTEENN
from sklearn.feature_selection import SelectKBest, RFE, SelectFromModel
from sklearn.feature_selection import f_classif, mutual_info_classif
from sklearn.ensemble import RandomForestClassifier
import src.utils.Utils as utils

def get_input_example(X_train):
    import pandas as pd
    if isinstance(X_train, pd.DataFrame):
        return X_train.head(1)
    else:
        return X_train[0:1]
def train_lightgbm(X_train, y_train, X_test, y_test, collumn_target, base, city, balancing_method=None,feature_selection_method=None, feature_selection_order='after',n_trials=50, fase=2):
    max_features = X_train.shape[1]
    feature_selection_methods = {
                'SelectKBest': SelectKBest(score_func=f_classif, k=int(max_features * 0.5)),
                'RFE': RFE(estimator=RandomForestClassifier(n_estimators=50,   
                    max_depth=10,       
                    random_state=42,
                    n_jobs=-1)),
                'SelectFromModel': SelectFromModel(estimator=RandomForestClassifier(n_estimators=50))
            }
    balancing_methods = {
                    'SMOTE': SMOTE(
                        random_state=42,
                    ),
                    'ADASYN': ADASYN(
                        random_state=42,
                    ),
                    'RandomUnderSampler': RandomUnderSampler(
                        random_state=42,
                    ),
                    'SMOTEENN': SMOTEENN(
                        n_jobs=-1
                    )
                }
    try:

        def objective(trial):
            params = {
                'num_leaves': trial.suggest_int('num_leaves', 20, 100),
                'max_depth': trial.suggest_int('max_depth', -1, 15),
                'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3),
                'n_estimators': trial.suggest_categorical('n_estimators', [100, 200, 500]),
                'min_gain_to_split': trial.suggest_float('min_gain_to_split', 0.0, 0.1),
                'random_state': 42
            }
            X_train_copy, y_train_copy ,X_test_copy= X_train.copy(), y_train.copy(),X_test.copy()
            balanceador  = None
            feature_selector = None
            # Apply feature selection and balancing methods if specified
            if feature_selection_method is not None and balancing_method is not None:
                if feature_selection_order == 'before':
                    # First apply feature selection, then balancing
                    feature_selector = feature_selection_methods[feature_selection_method]
                    X_train_copy = feature_selector.fit_transform(X_train_copy, y_train_copy)
                    X_test_copy = feature_selector.transform(X_test_copy)
                    balanceador = balancing_methods[balancing_method]
                    X_train_copy, y_train_copy = balanceador.fit_resample(X_train_copy, y_train_copy)
                else:
                    # First apply balancing, then feature selection
                    balanceador = balancing_methods[balancing_method]
                    X_train_copy, y_train_copy = balanceador.fit_resample(X_train_copy, y_train_copy)
                    feature_selector = feature_selection_methods[feature_selection_method]
                    X_train_copy = feature_selector.fit_transform(X_train_copy, y_train_copy)
                    X_test_copy = feature_selector.transform(X_test_copy)
            # If only balancing method or feature selection method is specified
            else:
                if balancing_method is not None:
                    balanceador = balancing_methods[balancing_method]
                    X_train_copy, y_train_copy = balanceador.fit_resample(X_train_copy, y_train_copy)
                if feature_selection_method is not None:
                    feature_selector = feature_selection_methods[feature_selection_method]
                    X_train_copy = feature_selector.fit_transform(X_train_copy, y_train_copy)
                    X_test_copy = feature_selector.transform(X_test_copy)


            model = lgb.LGBMClassifier(**params)
            model.fit(X_train, y_train)
            preds = model.predict(X_test)
            return recall_score(y_test, preds)

        experiment_name = f"DataBase_{base}_fase_{fase}"
        mlflow.set_experiment(experiment_name)
        start_time = time.time()

        with mlflow.start_run(run_name=f"LightGBM Optuna - {city}"):
            study = optuna.create_study(direction='maximize')
            study.optimize(objective, n_trials=n_trials)

            best_params = study.best_params

            lightgbm_params = {k: v for k, v in study.best_params.items() if k in [
                'num_leaves',
                'max_depth',
                'learning_rate',
                'n_estimators',
                'min_gain_to_split',
            ]}
            balancer_params = {}
            feature_params = {}
            balancing_method = balancing_method if balancing_method else None

            if feature_selection_method is not None and balancing_method is not None:
                if feature_selection_order == 'before':
                    # First apply feature selection, then balancing
                    feature_selector = feature_selection_methods[feature_selection_method]
                    X_train = feature_selector.fit_transform(X_train, y_train)
                    X_test = feature_selector.transform(X_test)
                    balancer = balancing_methods[balancing_method]
                    X_train, y_train = balancer.fit_resample(X_train, y_train)
                else:
                    # First apply balancing, then feature selection
                    balancer = balancing_methods[balancing_method]
                    X_train, y_train = balancer.fit_resample(X_train, y_train)
                    feature_selector = feature_selection_methods[feature_selection_method]
                    X_train = feature_selector.fit_transform(X_train, y_train)
                    X_test = feature_selector.transform(X_test)
            else:
                # If only balancing method or feature selection method is specified
                if feature_selection_method is not None:
                    feature_selector = feature_selection_methods[feature_selection_method]
                    X_train = feature_selector.fit_transform(X_train, y_train)
                    X_test = feature_selector.transform(X_test)
                if balancing_method is not None:
                    balancer = balancing_methods[balancing_method]
                    X_train, y_train = balancer.fit_resample(X_train, y_train)

            best_model = lgb.LGBMClassifier(**best_params, random_state=42)
            best_model.fit(X_train, y_train)
            y_pred = best_model.predict(X_test)

            auc_roc = roc_auc_score(y_test, best_model.predict_proba(X_test)[:, 1])
            precision = precision_score(y_test, y_pred)
            recall = recall_score(y_test, y_pred)
            f1 = f1_score(y_test, y_pred)
            
            # Log parameters and metrics to MLflow
            mlflow.log_param("collumn_target", collumn_target)
            mlflow.log_param("base", base)
            mlflow.log_param("city", city)

            mlflow.log_param("balancing_method", balancing_method if balancing_method else "None")
            mlflow.log_param("feature_selection_method", feature_selection_method if feature_selection_method else "None")
            mlflow.log_param("feature_selection_order", feature_selection_order)
            # log best parameters of model
            mlflow.log_params(lightgbm_params)
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
                artifact_path="lightgbm_model",
                input_example=get_input_example(X_train)
            )

            # Matriz de confusão
            cm = confusion_matrix(y_test, y_pred)
            plt.figure(figsize=(6, 4))
            sns.heatmap(cm, annot=True, fmt='d', cmap='Blues',
                        xticklabels=['Negativo', 'Positivo'],
                        yticklabels=['Negativo', 'Positivo'])
            plt.xlabel('Predito')
            plt.ylabel('Real')
            plt.title('Matriz de Confusão')

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
                fig_hist.figure.savefig(hist_path, bbox_inches='tight')
                mlflow.log_artifact(hist_path, artifact_path="plots")

                # Hyperparameter importance plot
                fig_importance = optuna_viz.plot_param_importances(study)
                fig_importance.figure.set_size_inches(8, 5)
                imp_path = os.path.join(temp_dir, "hyperparameter_importance.png")
                fig_importance.figure.savefig(imp_path, bbox_inches='tight')
                mlflow.log_artifact(imp_path, artifact_path="plots")

            plt.close()

    except Exception as e:
        print(f"Erro ao treinar o modelo : {e}")
