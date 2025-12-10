import optuna
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_selection import (
    RFE,
    SelectFromModel,
    SelectKBest,
    f_classif,
    mutual_info_classif,
)

score_func_map = {"f_classif": f_classif, "mutual_info_classif": mutual_info_classif}


def configure_selectkbest(trial, max_features):

    return SelectKBest(
        score_func=score_func_map[
            trial.suggest_categorical("score_func", list(score_func_map.keys()))
        ],
        k=trial.suggest_int("k", 3, max_features),
    )


def configure_rfe(trial, max_features):
    estimator = RandomForestClassifier(
        n_estimators=50, max_depth=10, random_state=42, n_jobs=-1
    )

    return RFE(
        estimator=estimator,
        n_features_to_select=trial.suggest_int("n_features_to_select", 3, max_features),
        step=1,
    )


def configure_selectfrommodel(trial, max_features):
    estimator = RandomForestClassifier(
        n_estimators=100, max_depth=15, random_state=42, n_jobs=-1
    )

    return SelectFromModel(
        estimator=estimator,
        threshold=trial.suggest_float("threshold", 0.001, 0.05),
        prefit=False,
        max_features=trial.suggest_int("max_features", 3, max_features),
    )


def create_selectkbest(**params):
    func_name = params.get("score_func", "f_classif")

    # Converta para a função real
    score_func = score_func_map.get(func_name, f_classif)
    return SelectKBest(score_func=score_func, k=params.get("k", 20))


def create_rfe(**params):
    estimator = RandomForestClassifier(
        n_estimators=params.get("n_estimators", 50),
        max_depth=params.get("max_depth", 10),
        random_state=42,
        n_jobs=-1,
    )

    return RFE(
        estimator=estimator,
        n_features_to_select=params.get("n_features", 15),
        step=params.get("step", 1),
    )


def create_selectfrommodel(**params):
    estimator = RandomForestClassifier(
        n_estimators=params.get("n_estimators", 100),
        max_depth=params.get("max_depth", 15),
        random_state=42,
        n_jobs=-1,
    )

    return SelectFromModel(
        estimator=estimator, threshold=params.get("threshold", 0.01), prefit=False
    )


feature_selection_methods = {
    "SelectKBest": configure_selectkbest,
    "RFE": configure_rfe,
    "SelectFromModel": configure_selectfrommodel,
}
feature_selection_creators = {
    "SelectKBest": create_selectkbest,
    "RFE": create_rfe,
    "SelectFromModel": create_selectfrommodel,
}
