import optuna 
import os
import time
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
from imblearn.over_sampling import SMOTE, ADASYN
from imblearn.under_sampling import RandomUnderSampler
from imblearn.combine import SMOTEENN


load_dotenv()
mlflow_tracking_uri = os.getenv('MLFLOW_TRACKING_URI')
"""
Balanced Methods:
- SMOTE
- ADASYN
- RandomUnderSampler
- SMOTEENN
"""
def train_decision_tree(X_train, y_train, X_test, y_test, collumn_target,base,city,balancing_method=None):
    metodos_balanceamento = {
        'SMOTE': SMOTE(random_state=42),
        'ADASYN': ADASYN(random_state=42),
        'RandomUnderSampler': RandomUnderSampler(random_state=42),
        'SMOTEENN': SMOTEENN(random_state=42)
    }
    try:
        if balancing_method in metodos_balanceamento and balancing_method is not None: 
            X_train, y_train = metodos_balanceamento[balancing_method].fit_resample(X_train, y_train)


        
        def objective(trial):
            params = {
                'max_depth': trial.suggest_int('max_depth', 2, 50),
                'min_samples_split': trial.suggest_int('min_samples_split', 2, 10),
                'min_samples_leaf': trial.suggest_int('min_samples_leaf', 1, 10),
                'max_features': trial.suggest_categorical('max_features', [None, 'sqrt', 'log2'])
            }

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
            best_model = DecisionTreeClassifier(**best_params, random_state=42)
            best_model.fit(X_train, y_train)
            y_pred = best_model.predict(X_test)

            auc_roc = roc_auc_score(y_test, best_model.predict_proba(X_test)[:, 1])
            precision = precision_score(y_test, y_pred)
            recall = recall_score(y_test, y_pred)
            f1 = f1_score(y_test, y_pred)

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
            plt.close()

    except Exception as e:
        print(f"Erro ao treinar o modelo: {e}")
