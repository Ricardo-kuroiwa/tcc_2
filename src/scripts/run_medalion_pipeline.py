from src.data_transform.bronze_processor import  main as load_raw_sources
from src.data_transform.silver_processor import main as clean_and_transform
from src.data_transform.gold_processor import main as build_features

from src.utils.logger import setup_logger
import time
logger = setup_logger("silver_processor")
def run_pipeline():
    try:
        logger.info("Executando Bronze Layer.....")
        load_raw_sources()
        
        logger.info("Executando Silver Layer.....")   
        clean_and_transform()
        
        logger.info("Executando Gold Layer.....")    
        build_features()
        
        logger.info("Pipeline concluído com sucesso!")
    except Exception as e:
        logger.error(f"Erro durante a execução do pipeline: {e}")
        raise

if __name__ == "__main__":
    start_time = time.time()
    run_pipeline()
    end_time = time.time()
    logger.info(f"Tempo total de execução: {end_time - start_time:.2f} segundos")