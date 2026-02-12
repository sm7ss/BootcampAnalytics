import polars as pl 
import psutil
import logging
from pathlib import Path

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s-%(levelname)s-%(message)s')
logger= logging.getLogger(__name__)

def get_frame(file: Path, overhead: float=1.8) -> pl.DataFrame: 
    memory_available= psutil.virtual_memory().available
    estimated_memory= file.stat().st_size * overhead
    
    ratio= estimated_memory/memory_available
    
    if ratio > 0.65: 
        logger.error(f'The file {file.name} is too large to be process. Ratio: {ratio:.2f}')
        raise ValueError(f'The file {file.name} is too large to be process. Ratio: {ratio:.2f}')
    else: 
        logger.info(f'The frame for the file {file.name} was obtained succesfully. Ratio: {ratio:.2f}')
        return pl.read_csv(file)

