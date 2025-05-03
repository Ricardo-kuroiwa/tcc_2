## Descrição do projeto

Este projeto coleta dados climáticos e de desastres naturais de diversas fontes e realiza uma análise exploratória para identificar padrões e tendências relevantes.

### Fontes de dados meteorológicos:
- **base_1:** OpenMeteo
- **base_2:** NOAA
- **base_3:** Meteostat
- **base_disaster:** NCDC NOAA
### Cidades monitorada:
- Dallas, TX
- Houston, TX
- Miami, FL
- Nashville, TN
- New York, NY
- Oklahoma City, OK
- Albuquerque, NM
- Chicago, IL

### Modelos escolhidos
- Decision Tree
- XGBoost
- LightGBM

### Feature Selection
- SelectKBest

## Instruções para Execução

### 1 . Criar ambiente virtual
Abra o terminal e execute o seguinte comando:
py -m venv venv
### 2 . Iniciar ambiente virtual
Execute o seguinte comando no terminal
venv\Scripts\activate
### 3 . Instalar as dependencias  
pip install -r requirements.txt
### 4 . Rodar o projeto
py main.py