from pathlib import Path
from typing import Dict, Any
import yaml
import tomli

from .validate_config import validation
import logging

logging.basicConfig(level=logging.INFO, format='%(actime)s-%(levelname)s-&(message)s')
logger= logging.getLogger(__name__)

class ReadFile: 
    def __init__(self):
        config_dir = Path(__file__).parent.parent.parent / 'config' 
        self.path= None
        
        for ext in ['.yml', '.yaml', '.toml']: 
            potential_possible_path= config_dir / f'config{ext}'
            if potential_possible_path.exists(): 
                self.path = potential_possible_path
                break
        
        if self.path is None: 
            logger.error(f'No config file found in {config_dir} with ".yaml", ".yml" or ".toml"')
            raise FileNotFoundError(f'No config file found in {config_dir} with ".yaml", ".yml" or ".toml"')
    
    def read_toml(self) -> Dict[str, Any]: 
        try: 
            with open(self.path, 'rb') as f: 
                read= tomli.load(f)
                logger.info('The TOML file was succesfully read')
            validation_config= validation(**read)
            logger.info('The TOML file was sucessfully validated')
            return validation_config
        except tomli.TOMLDecodeError: 
            logger.error(f'The file {self.path.name} is corrupt')
            raise ValueError(f'The file {self.path.name} is corrupt')
        except Exception as e: 
            logger.error(f'An error ocurred while reading the file {self.path.name}:\n{e}')
            raise ValueError(f'An error ocurred while reading the file {self.path.name}:\n{e}')
    
    def read_yaml(self) -> Dict[str, Any]: 
        try:
            with open(self.path, 'r') as f: 
                read= yaml.safe_load(f)
                logger.info('The YAML file was sucessfully read')
            validation_config= validation(**read)
            logger.info('The YAML file was sucessfully validated')
            return validation_config
        except yaml.YAMLError: 
            logger.error(f'The file {self.path.name} is corrupt')
            raise ValueError(f'The file {self.path.name} is corrupt')
        except Exception as e:
            logger.error(f'An error ocurred while reading the file {self.path.name}:\n{e}')
            raise ValueError(f'An error ocurred while reading the file {self.path.name}:\n{e}')
    
    def read_file(self) -> Dict[str, Any]: 
        if self.path.suffix in ['.yml', '.yaml']: 
            return self.read_yaml()
        elif self.path.suffix == '.toml': 
            return self.read_toml()

