from imblearn.over_sampling import SMOTE, ADASYN
from imblearn.under_sampling import RandomUnderSampler
from imblearn.combine import SMOTEENN
from imblearn.combine import SMOTETomek
import optuna

def configure_smote(trial):
    return SMOTE(
        k_neighbors=trial.suggest_int('k_neighbors', 3, 10),
        sampling_strategy=trial.suggest_float('sampling_strategy', 0.5, 1.0),
        random_state=42
    )

def configure_adasyn(trial):
    return ADASYN(
        n_neighbors=trial.suggest_int('n_neighbors', 3, 10),
        sampling_strategy=trial.suggest_float('sampling_strategy', 0.5, 1.0),
        random_state=42
    )

def configure_randomUnderSampler(trial):
    return RandomUnderSampler(
        sampling_strategy=trial.suggest_float('sampling_strategy', 0.3, 1.0),
        random_state=42
    )

def configure_smoteenn(trial):
    smote = SMOTE(
        k_neighbors=trial.suggest_int('k_neighbors', 3, 10),
        sampling_strategy=trial.suggest_float('sampling_strategy', 0.5, 1.0),
        random_state=42
    )
    return SMOTEENN(smote=smote, random_state=42)
def configure_smotetomek(trial):
    smote = SMOTE(
        k_neighbors=trial.suggest_int('k_neighbors', 3, 10),
        sampling_strategy=trial.suggest_float('sampling_strategy', 0.5, 1.0),
        random_state=42
    )
    return SMOTETomek(smote=smote, random_state=42)

# Funções para criar instâncias com parâmetros já definidos
def create_smote(**params):
    return SMOTE(
        k_neighbors=params.get('k_neighbors', 5),
        sampling_strategy=params.get('sampling_strategy', 'auto'),
        random_state=42
    )

def create_adasyn(**params):
    return ADASYN(
        n_neighbors=params.get('n_neighbors', 5),
        sampling_strategy=params.get('sampling_strategy', 'auto'),
        random_state=42
    )

def create_randomUnderSampler(**params):
    return RandomUnderSampler(
        sampling_strategy=params.get('sampling_strategy', 'auto'),
        random_state=42
    )

def create_smoteenn(**params):
    smote = SMOTE(
        k_neighbors=params.get('k_neighbors', 5),
        sampling_strategy=params.get('sampling_strategy', 'auto'),
        random_state=42
    )
    return SMOTEENN(smote=smote, random_state=42)
def create_smotetomek(**params):
    smote = SMOTE(
        k_neighbors=params.get('k_neighbors', 5),
        sampling_strategy=params.get('sampling_strategy', 'auto'),
        random_state=42
    )
    return SMOTETomek(smote=smote, random_state=42)
# Dicionários para diferentes usos
balancing_methods = {
    'SMOTE': configure_smote,
    'ADASYN': configure_adasyn,
    'RandomUnderSampler': configure_randomUnderSampler,
    'SMOTEENN': configure_smoteenn,
    'SMOTETomek': configure_smotetomek,
}

balancing_creators = {
    'SMOTE': create_smote,
    'ADASYN': create_adasyn,
    'RandomUnderSampler': create_randomUnderSampler,
    'SMOTEENN': create_smoteenn,
    'SMOTETomek': create_smotetomek,
}