import plotly.express as px
import polars as pl 
from typing import Dict, Any, Union, List

import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s-%(levelname)s-%(message)s')
logger= logging.getLogger(__name__)

class DistributionData: 
    def __init__(self, frame: pl.DataFrame, data_analysis: Dict[str, Any], analysis_config: Dict[str, Any]):
        self.frame= frame
        self.analysis_config= analysis_config
        self.columns= data_analysis.insight_questions[0]['columns']
    
    def graph(self, col: str, n_bins: int): 
        return px.histogram(
            self.frame, 
            x=col, 
            nbins=n_bins, 
            title=f'Distribution: {col}'
        )
    
    def save_plot(self, fig, name: str) -> None: 
        folder= self.analysis_config.output.report_folder_name
        path= folder / name
        fig.write_html(path, include_plotlyjs="cdn")
        return path
    
    def num_insights(self, col: str) -> Dict[str, Any]:
        stat= self.frame[col].describe()
        q5, q95= self.frame[col].quantile(0.05), self.frame[col].quantile(0.95)
        
        num_data= {
            'type': 'numeric', 
            'mean': float(stat.filter(pl.col('statistic')=='mean')['value'].item()), 
            'std': float(stat.filter(pl.col('statistic')=='std')['value'].item()), 
            'iqr_5_95': [float(q5), float(q95)], 
            'min': float(stat.filter(pl.col('statistic')=='min')['value'].item()), 
            'max': float(stat.filter(pl.col('statistic')=='max')['value'].item())
        }
        return num_data
    
    def cat_insights(self, col: str) -> Dict[str, Any]: 
        cat_insights= {
            'type': 'categorical', 
            'unique_count': self.frame[col].n_unique()
        }
        
        return cat_insights
    
    def run_distribution(self) -> Dict[str, Any]: 
        num= [col for col in self.frame.select(pl.selectors.numeric()).columns]
        cat= [col for col in self.frame.select(pl.selectors.string()).columns]
        
        n_bins= self.analysis_config.plots.histogram_bins
        
        insights= {}
        dict_data= {}
        
        for col in self.columns: 
            if col in num: 
                dict_data[col]= self.num_insights(col=col)
                
                fig= self.graph(col=col, n_bins=n_bins)
                dict_data[col]['plots']= str(self.save_plot(fig=fig, name=f'distribution_{col}.html'))
            elif col in cat: 
                dict_data[col]= self.cat_insights(col=col)
            else: 
                continue
        
        insights['distribution']=dict_data
        return dict_data



class AnalsisData: 
    def __init__(self, frame: pl.DataFrame, data_analysis: Dict[str, Any], analysis_config: Dict[str, Any]):
        self.frame= frame
        self.analysis_config= analysis_config
        self.data_analysis= data_analysis