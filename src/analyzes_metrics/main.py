import src.utils.Utils as utils
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os 
collmns=['run_id', 'status', 'run_name', 'model_name', 'balancing_method',
       'base', 'city', 'collumn_target', 'colsample_bytree', 'learning_rate',
       'max_depth', 'n_estimators', 'subsample', 'auc_roc', 'f1_score',
       'precision', 'recall', 'training_time', 'min_gain_to_split',
       'num_leaves', 'max_features', 'min_samples_leaf', 'min_samples_split']
def calculate_gain_collumn(df: pd.DataFrame,collumn) -> pd.DataFrame:
    if collumn not in df.columns:
        raise ValueError(f"A coluna '{collumn}' não existe no DataFrame.")
    if df[collumn].dtype != 'float64':
        raise ValueError(f"A coluna '{collumn}' não é do tipo numerico")
    min_value = df[collumn].min()
    df['gain_to_min_value'] = df[collumn].apply(lambda x: (x) / min_value)
    return df
def show_top_models(df: pd.DataFrame, metric: str = "recall", top_n: int = 10):
    """
    Exibe os top N modelos com maior valor de uma métrica.
    """
    top_df = df.sort_values(by=metric, ascending=False).head(top_n)
    print(f"\nTop {top_n} modelos por {metric}:")
    print(top_df[[ 'model_name', 'balancing_method', metric, 'precision', 'recall','auc_roc','training_time','base','city']])

def show_compare_balancing_by_city(df: pd.DataFrame, path:str,metric: str = "f1_score"):
    """
    Compara modelos por cidade e calcula o ganho percentual do melhor para o pior.
    """
    distinct_cities = df['city'].unique()
    print(f"Cidades disponíveis: {distinct_cities}")

    for city in distinct_cities:
        df_city = df[df['city'] == city]
        print(f"\nAnalisando a cidade: {city}")

        if df_city.empty:
            print(f"Nenhuma run encontrada para a cidade '{city}'.")
            continue

        # Agrupar por modelo
        df_grouped = df_city.groupby('model_name')

        for model_name, group in df_grouped:
            print(f"\nModelo: {model_name}")

            # Garantir que a métrica existe no grupo
            if metric not in group.columns:
                print(f"Métrica '{metric}' não encontrada para o modelo '{model_name}'")
                continue

            # Selecionar colunas relevantes
            group = group[['balancing_method', metric]].copy()

            # Calcular ganho percentual
            group = calculate_gain_collumn(group, metric)

            # Ordenar do melhor para o pior
            group = group.sort_values(by=metric, ascending=False)

            # Exibir
            print(group)

            
            plt.figure(figsize=(10, 5))
            ax = sns.barplot(x=group['balancing_method'], y=group[metric], palette='hls')
            for p, gain in zip(ax.patches, group['gain_to_min_value']):
                height = p.get_height()
                ax.text(x=p.get_x() + p.get_width() / 2,
                        y=height + 0.02 * height,
                        s=f"{gain*100:.2f}%",
                        ha='center')
            ax.set_title(f'Comparação de {metric} por cidade: {city} - Modelo: {model_name}')
            ax.set_xlabel('Metodo de Balanceamento')
            ax.set_ylabel(f'Metrica {metric}')
            utils.save_plot(f'analyzes_metrics/compare_balancing_by_city/{path}/', f"{city}_{model_name}_{metric}.png")

def show_scatter_performance(df, base: str, metric='f1_score', time_col='training_time'):
    """
    Plota gráfico de dispersão entre desempenho (métrica) e tempo de treinamento, por cidade.

    Parâmetros:
    - df: DataFrame com os dados do experimento.
    - base: Nome da base (usado para salvar a imagem).
    - metric: Nome da métrica de desempenho ('f1_score', 'auc_roc', etc).
    - time_col: Nome da coluna que representa o tempo de treinamento.
    """
    distinct_cities = df['city'].unique()

    for city in distinct_cities:
        df_city = df[df['city'] == city]

        if df_city.empty:
            print(f"Nenhuma run encontrada para a cidade '{city}'.")
            continue

        plt.figure(figsize=(10, 6))

        # Gráfico de dispersão por cidade
        sns.scatterplot(
            data=df_city,
            x=time_col,
            y=metric,
            hue='balancing_method',
            style='model_name',
            palette='Set2',
            s=100
        )

        plt.title(f'Desempenho na Cidade: {city} ({metric})')
        plt.xlabel('Tempo de Treinamento (s)')
        plt.ylabel(metric)
        plt.legend(title='Método de Balanceamento', bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.tight_layout()

        # Salvar o gráfico usando a função utilitária (assumindo que ela existe)
        utils.save_plot(f'analyzes_metrics/scatter_performance/{base}', f'{city}_scatter_performance_{metric}.png')
        plt.close()
if __name__=="__main__":
    folder = "src/analyzes_metrics/data/"
    files_in_folder = os.listdir(folder)
    for file in files_in_folder:
        print(f"Arquivo: {file}")
        print(f"Carregando arquivo: {os.path.splitext(file)[0]}")
        df = utils.read_data_from_parquet(os.path.join(folder, file))
        #show_compare_balancing_by_city(df,os.path.splitext(file)[0], metric="recall")
        #show_top_models(df, metric="recall", top_n=10)
        show_scatter_performance(df,os.path.splitext(file)[0], metric='recall', time_col='training_time')
