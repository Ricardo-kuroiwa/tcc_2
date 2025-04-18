import pandas as pd
import numpy as np
import os

# Caminho da pasta
pasta = './data/raw/base_disaster/'

# Listar todos os arquivos da pasta
lista = []
arquivos = os.listdir(pasta)
for file in arquivos:
    print(f'Arquivo: {file}')
    try:
        data = pd.read_csv(os.path.join(pasta, file), on_bad_lines='skip')  # Ignora linhas com problemas
        data.drop('EVENT_NARRATIVE', axis=1, inplace=True)  # Remove a coluna EVENT_NARRATIVE
        print(f'Linhas: {data.shape[0]}')
        
        # Obter valores únicos da coluna 'EVENT_TYPE'
        unique_values = data["EVENT_TYPE"].unique()
        
        # Adicionar cada valor único na lista (sem duplicatas)
        for i in unique_values:
            if i not in lista:
                lista.append(i)

    except Exception as e:
        print(f"Erro ao ler o arquivo {file}: {e}")

print(f'Eventos únicos encontrados: {lista}')

events = [
    "Astronomical Low Tide", "Avalanche", "Blizzard", "Coastal Flood", "Cold/Wind Chill",
    "Debris Flow", "Dense Fog", "Dense Smoke", "Drought", "Dust Devil", "Dust Storm",
    "Excessive Heat", "Extreme Cold/Wind Chill", "Flash Flood", "Flood", "Freezing Fog",
    "Frost/Freeze", "Funnel Cloud", "Hail", "Heat", "Heavy Rain", "Heavy Snow", "High Surf",
    "High Wind", "Hurricane (Typhoon)", "Ice Storm", "Lake-Effect Snow", "Lakeshore Flood",
    "Lightning", "Marine Hail", "Marine High Wind", "Marine Strong Wind", "Marine Thunderstorm Wind",
    "Rip Current", "Seiche", "Sleet", "Sneakerwave", "Storm Surge/Tide", "Strong Wind",
    "Thunderstorm Wind", "Tornado", "Tropical Depression", "Tropical Storm", "Tsunami",
    "Volcanic Ash", "Waterspout", "Wildfire", "Winter Storm", "Winter Weather"
]

# Verificar a diferença entre os eventos encontrados nos arquivos e os eventos esperados
missing_events = np.setdiff1d(events, lista)  # Eventos esperados mas não encontrados
extra_events = np.setdiff1d(lista, events)   # Eventos encontrados mas não esperados

print(f'Eventos ausentes (não encontrados nos arquivos): {missing_events}')
print(f'Eventos extras (não na lista de eventos conhecidos): {extra_events}')
