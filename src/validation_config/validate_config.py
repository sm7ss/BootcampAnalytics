from pydantic import field_validator, model_validator, Field, BaseModel
from typing import List, Dict, Any, Literal
import plotly.express as px 
from pathlib import Path
import polars as pl
from datetime import datetime

from ..strategies.strategies import encoding_strategy, plotly_templates_strategy, outliers_strategy
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

class thresholds_validation(BaseModel): 
    null_threshold: float= Field(gt=0.01, le=1.00)

class eda_validation(BaseModel): 
    thresholds: thresholds_validation

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

class output_validation(BaseModel): 
    report_folder_name: str
    save_plots: bool
    insights_json_name: str
    save_insights: bool
    
    @field_validator('report_folder_name')
    def report_name_validation(cls, v): 
        path= Path(v)
        
        path.mkdir(parents=True, exist_ok=True)
        return path
    
    @field_validator('insights_json_name')
    def paths_and_file_validation(cls, v): 
        path= Path(v)
        
        if path.suffix != '.json': 
            logger.error(f'The file {path.name} should be a JSON file')
            raise ValueError(f'The file {path.name} should be a JSON file')
        
        today_date= datetime.now()
        format_date= today_date.strftime('%Y-%m-%d')
        
        return f'{path.stem}_{format_date}.json'

class analysis_config_validation(BaseModel): 
    plots: plots_validation
    output: output_validation

class data_analysis_validation(BaseModel): 
    representative_columns: List[str]
    auto_insights: bool
    insight_questions: List[Dict[str, Any]]

class validation(BaseModel): 
    data: data_validation
    eda: eda_validation
    analysis_config: analysis_config_validation
    data_analysis: data_analysis_validation
    
    @model_validator(mode='after')
    def columns_existence(self): 
        columnas= self.data_analysis.representative_columns
        file= self.data.input_path
        
        frame= pl.read_csv(file, n_rows=10)
        
        columns_frame= frame.columns
        cat= [col for col in frame.select(pl.selectors.string()).columns]
        num= [col for col in frame.select(pl.selectors.numeric()).columns]
        
        if isinstance(columnas, list): 
            for col in columnas: 
                if col not in columns_frame: 
                    logger.error(f'The column {col} is not in the schema. Existing columns:\n{columns}')
                    raise ValueError(f'The column {col} is not in the schema. Existing columns:\n{columns}')
        elif isinstance(columnas, str): 
            if columnas not in columns_frame: 
                logger.error(f'The column {columnas} is not in the schema. Existing columns:\n{columns}')
                raise ValueError(f'The column {columnas} is not in the schema. Existing columns:\n{columns}')
        
        insight_questions= self.data_analysis.insight_questions
        for i in range(len(insight_questions)):
            id_insights= insight_questions[i].get('id')
            enable= insight_questions[i].get('enable')
            columns= insight_questions[i].get('columns')
            
            if enable is None: 
                logger.eror(f'There must be an "enable" field in {id_insights}')
                raise ValueError(f'There must be an "enable" field in {id_insights}')
            if not isinstance(columns, list): 
                logger.error('The "columns" field must be a list')
                raise ValueError('The "columns" field must be a list')

            if not columns: 
                if id_insights == 'distribution':
                    logger.warning(f'There must be a "columns" field in {id_insights}. The analysis will have all the columns')
                    insight_questions[i]['columns']=columns_frame 
                elif id_insights == 'outliers': 
                    logger.warning(f'There must be a "columns" field in {id_insights}. The analysis will have all the numeric columns')
                    insight_questions[i]['columns']= num
                elif id_insights == 'correlation': 
                    logger.warning(f'There must be a "columns" field in {id_insights}. The analysis will have 2 numeric columns or more if exists')
                    total_num_col= len(num)
                    if total_num_col >=2: 
                        insight_questions[i]['columns']= num
                        logger.warning(f'There are {total_num_col} numeric columns that will use for correlation')
                    else: 
                        logger.warning(f'There are fewer than 2 numerical columns to obtain the correlation. Insights will not be obtained')
                        insight_questions[i]['enable']= False
                elif id_insights == 'category_dominance': 
                    logger.warning(f'There must be a "columns" field in {id_insights}. The analysis will have all the columns')
                    insight_questions[i]['columns']= cat
                else: 
                    logger.error(f'There cannot be fields other than: "distribution", "outliers", "correlation", and "category_dominance"')
                    raise ValueError(f'There cannot be fields other than: "distribution", "outliers", "correlation", and "category_dominance"')
            else: 
                for col in columns: 
                    if col not in columns_frame: 
                        logger.error(f'The column {columnas} is not in the schema. Existing columns:\n{columns}')
                        raise ValueError(f'The column {columnas} is not in the schema. Existing columns:\n{columns}')
            
            if id_insights == 'distribution': 
                continue
            elif id_insights == 'outliers': 
                method= insight_questions[i].get('method')
                if not method: 
                    logger.error(f'There must be a "method" field for {id_insights}')
                    raise ValueError(f'There must be a "method" field for {id_insights}')
            elif id_insights == 'correlation': 
                min_numeric_col= insight_questions[i].get('min_numeric_col')
                if not min_numeric_col: 
                    logger.error(f'There must be a "min_numeric_col" field for {id_insights}')
                    raise ValueError(f'There must be a "min_numeric_col" field for {id_insights}')
            elif id_insights == 'category_dominance': 
                top_n= insight_questions[i].get('top_n')
                rare_threshold= insight_questions[i].get('rare_threshold')
                if not top_n: 
                    logger.error(f'There must be a "top_n" field in {id_insights}')
                    raise ValueError(f'There must be a "top_n" field in {id_insights}')
                if not rare_threshold: 
                    logger.error(f'There must be a "rare_threshold" field in {id_insights}')
                    raise ValueError(f'There must be a "rare_threshold" field in {id_insights}')
            else: 
                logger.error(f'There cannot be fields other than: "distribution", "outliers", "correlation", and "category_dominance". You write {id_insights}')
                raise ValueError(f'There cannot be fields other than: "distribution", "outliers", "correlation", and "category_dominance". You write {id_insights}')
        
        return self

