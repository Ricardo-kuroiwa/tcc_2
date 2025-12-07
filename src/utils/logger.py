import logging.config
import yaml
import os

def setup_logger(name="default", config_path="configs/logging_config.yml") -> logging.Logger:
    """
    Configura e retorna um logger com base no arquivo YAML.

    :param name: Nome do logger conforme definido no YAML
    :param config_path: Caminho para o arquivo de configuração YAML
    :return: Instância do logger
    """
    os.makedirs("logs", exist_ok=True)

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    logging.config.dictConfig(config)
    return logging.getLogger(name)