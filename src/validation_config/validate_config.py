from pydantic import field_validator, model_validator, Field, BaseModel
from typing import List, Union
import plotly.express as px 
from pathlib import Path
import polars as pl

from ..strategies.strategies import encoding_strategy, plotly_templates_strategy
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s-%(levelname)s-%(message)s')
logger= logging.getLogger(__name__)

class data_validation(BaseModel): 
    input_path: str
    sample: float= Field(ge=0.01, le=1.00)
    encoding: encoding_strategy
    
    @field_validator('input_path')
    def input_path_validation(cls, v): 
        path= Path(v)
        
        if not path.exists(): 
            logger.error(f'The file {path.name} does not exist')
            raise FileNotFoundError(f'The file {path.name} does not exist')
        
        if path.suffix != '.csv': 
            logger.error('Only CSV files are supported')
            raise ValueError('Only CSV files are supported')        
        return path

class plots_validation(BaseModel): 
    histogram_bins: int= Field(gt=0, le=30)
    color_palette: str
    plotly_template: plotly_templates_strategy
    
    @field_validator('color_palette')
    def color_palette_validation(cls, v): 
        palettes= set(dir(px.colors.qualitative)+dir(px.colors.sequential))
        if v not in palettes: 
            logger.error(f'The color {v} does not exist in the Plotly color palette. Available colors:\n{palettes}')
            raise ValueError(f'The color {v} does not exist in the Plotly color palette. Available colors:\n{palettes}')
        return v

class thresholds_validation(BaseModel): 
    null_threshold: float= Field(gt=0.01, le=1.00)

class output_validation(BaseModel): 
    report_name: str
    save_plots: bool
    
    @field_validator('report_name')
    def report_name_validation(cls, v): 
        path= Path(v)
        
        if path.suffix != '.txt': 
            logger.error(f'The file {path.name} must be an .txt file, not {path.suffix}')
            raise ValueError(f'The file {path.name} must be an .txt file, not {path.suffix}')
        return path

class eda_validation(BaseModel): 
    representative_columns: Union[List[str], str, None]
    plots: plots_validation
    thresholds: thresholds_validation
    output: output_validation

class validation(BaseModel): 
    data: data_validation
    eda: eda_validation
    
    @model_validator(mode='after')
    def columns_existence(self): 
        columnas= self.eda.representative_columns
        file= self.data.input_path
        
        schema= pl.scan_csv(file, n_rows=10).collect_schema()
        
        if isinstance(columnas, list): 
            for col in columnas: 
                if col not in schema: 
                    logger.error(f'The column {col} is not in the schema. Existing columns:\n{schema.keys}')
                    raise ValueError(f'The column {col} is not in the schema. Existing columns:\n{schema.keys}')
        elif isinstance(columnas, str): 
            if columnas not in schema: 
                logger.error(f'The column {columnas} is not in the schema. Existing columns:\n{schema.keys}')
                raise ValueError(f'The column {columnas} is not in the schema. Existing columns:\n{schema.keys}')
        else: 
            pass
        
        return self

