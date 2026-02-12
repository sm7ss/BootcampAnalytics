from .eda_analysis import EDA
from .data_analysis import AnalysisData

import polars as pl
import psutil
from typing import Dict, Any, List
import json
from pathlib import Path

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s-%(levelname)s-%(message)s')
logger= logging.getLogger(__name__)

class FormatDataAnalysis: 
    @classmethod
    def distribution_insight_format(cls, analysis_data: Dict[str, Any]) -> str: 
        text_format= ''
        
        for col in analysis_data: 
            ruta= analysis_data[col].get(f'analysis_path_{col}')
            mean= analysis_data[col].get('mean', None)
            
            if mean is None: 
                unique= analysis_data[col].get('unique_count')
                text_format+= f'- {col}:- unique values={unique}- Plot: {ruta}\n'
            
            median= analysis_data[col].get('median')
            std= analysis_data[col].get('std')
            skew= analysis_data[col].get('skew')
            
            text_format+= f'''- {col}: media={mean}, median={median}, std={std}, sesgo {skew}\n- Plot: {ruta}\n'''
        
        return text_format
    
    @classmethod
    def outlier_insight_format(cls, analysis_data: Dict[str, Any]) -> str: 
        text_format= ''
        
        for col in analysis_data: 
            n_out= analysis_data[col].get('total_outliers')
            pct_out= analysis_data[col].get('percent_outliers')
            ruta= ruta= analysis_data[col].get(f'analysis_path_{col}')
            
            if ruta is None: 
                text_format+= f'- The number of outliers is {n_out}, so it is not necessary to make a boxplot for column {col}\n'
            else: 
                text_format+= f'- {col}: {n_out} outliers -> ({pct_out:.2f}%)\n- Sample: {ruta}\n'
        
        return text_format
    
    @classmethod
    def correlation_insight_format(cls, analysis_data: Dict[str, Any]) -> str: 
        col_a= analysis_data['correlation'].get('top_correlation_a')
        col_b= analysis_data['correlation'].get('top_correlation_b')
        r_val= analysis_data['correlation'].get('r_value')
        ruta= analysis_data['correlation'].get('analysis_path')
        
        if ruta is None:
            return f"- The correlation value is invalid. Correlation detected: {r_val}\n"
        else:
            return f"- Strongest correlation: {col_a} vs {col_b} (r={r_val})\n- Complete matrix: {ruta}\n" 
    
    @classmethod
    def category_insight_format(cls, analysis_data: Dict[str, Any]) -> str: 
        text_format= ''
        
        for col in analysis_data: 
            top_label= analysis_data[col].get('top_labs')
            rare_count= analysis_data[col].get('rare_count')
            rare_threshold= analysis_data[col].get('rare_threshold')
            
            text_format += f'- {col}: top lables= {top_label}\n- {col}: {rare_count} rare categories (<{rare_threshold*100}%)\n'
        
        return text_format
    
    @classmethod
    def analysis_format(cls, path: Path, columns: List[str], load_json: Dict[str, Any]) -> str: 
        text= f'''
=== AUTOMATED INSIGHTS REPORT ===
Dataset: {path}
Columns: {columns}
        '''
        
        for analysis in load_json: 
            analysis_data= load_json[analysis]
            if analysis == 'distribution': 
                text+= '\n1. 📈 DISTRIBUTION\n'
                distribution_text= cls.distribution_insight_format(analysis_data=analysis_data)
                text+= distribution_text
            elif analysis == 'outliers': 
                text+= '\n2. ⚠️ OUTLIERS\n'
                outlier_text= cls.outlier_insight_format(analysis_data=analysis_data)
                text+= outlier_text
            elif analysis == 'correlation': 
                text+= '\n3. 🔗 CORRELATION\n'
                correlation_text= cls.correlation_insight_format(analysis_data=analysis_data)
                text+= correlation_text
            elif analysis == 'CategoryDominance': 
                text+= '\n4. 🏷️ CATEGORIES\n'
                cd_text= cls.category_insight_format(analysis_data=analysis_data)
                text+= cd_text
        
        return text

class Analysis: 
    def __init__(self, frame: pl.DataFrame, config: Dict[str, Any], overhead: float=1.8):
        self.config= config
        
        self.file= self.config.data.input_path
        self.null_th= self.config.eda.thresholds.null_threshold
        
        self.frame= frame
    
    def eda_analysis(self) -> None:
        eda_class= EDA(frame=self.frame, null_threshold=self.null_th)
        eda_class.run_eda()
    
    def data_analysis(self) -> None: 
        data_an= AnalysisData(frame=self.frame, config=self.config)
        path= data_an.run_analysis()
        encoding= self.config.data.encoding
        columns= self.frame.columns
        
        try: 
            with open(path, 'r', encoding=encoding) as f: 
                load_file= json.load(f)
            logger.info(f'The file {path.name} was readed succesfully')
        except Exception as e: 
            logger.error(f'An error occurred while reading the json file {path.name}. Error:\n{e}')
            raise f'An error occurred while reading the json file {path.name}. Error:\n{e}'
        
        print(FormatDataAnalysis.analysis_format(path=self.file.name, columns=columns, load_json=load_file))
    
    def run_analysis(self) -> None: 
        self.eda_analysis()
        self.data_analysis()



