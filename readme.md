## Execução automatizada com Taskipy

O projeto utiliza [Taskipy](https://github.com/illBeRoy/taskipy) para facilitar a execução dos principais pipelines e tarefas.

### Comandos disponíveis

Execute os comandos abaixo dentro do ambiente Poetry:

```
poetry run task bronze   # Executa o processamento da camada bronze
poetry run task silver   # Executa o processamento da camada silver
poetry run task gold     # Executa o processamento da camada gold
poetry run task pipeline # Executa todas as camadas em sequência (bronze, silver, gold)
```

Esses comandos estão definidos em `[tool.taskipy.tasks]` no `pyproject.toml`.

Caso queira adicionar novas tasks, basta editar essa seção.
## Descrição do projeto

Este projeto coleta dados climáticos e de desastres naturais de diversas fontes, realiza processamento de dados em múltiplas camadas (bronze, silver, gold) e aplica modelos de machine learning para análise e previsão de eventos extremos.

### Fontes de dados meteorológicos
- **base_1:** OpenMeteo
- **base_2:** NOAA
- **base_3:** Meteostat
- **base_disaster:** NCDC NOAA

### Cidades monitoradas
- Dallas, TX
- Houston, TX
- Miami, FL
- Nashville, TN
- New York, NY
- Oklahoma City, OK
- Albuquerque, NM
- Chicago, IL

### Modelos utilizados
- Decision Tree
- XGBoost
- LightGBM

### Seleção de Features
- SelectKBest
- RFE
- SelectFromModel

## Status Atual
- Estrutura de dados organizada em camadas (bronze, silver, gold)
- Pipelines automatizados para ingestão, transformação e validação
- Treinamento e avaliação de modelos com Optuna e MLflow
- Configuração e logging via arquivos YAML
- Ambiente gerenciado com Poetry

## Instruções para Execução (Poetry)

### 1. Instalar o Poetry
Se ainda não possui, instale o Poetry:
https://python-poetry.org/docs/#installation

### 2. Instalar dependências
No diretório do projeto, execute:
```
poetry install
```

### 3. Ativar o ambiente virtual do Poetry
```
poetry shell
```

### 4. Executar o pipeline principal
```
poetry run python src/scripts/run_medalion_pipeline.py
```

### 5. Treinar modelos
```
poetry run python src/train/main.py
```

### 6. Rodar testes
```
poetry run pytest
```

## Observações
- As configurações do projeto estão em `configs/config.yaml` e `configs/logging_config.yml`.
- Os dados devem estar nas pastas `data/raw`, `data/bronze`, `data/silver`, `data/gold`.
- Para reprocessar dados ou rodar experimentos, ajuste os scripts em `src/scripts/` e `src/train/` conforme necessário.