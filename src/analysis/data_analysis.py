import plotly.express as px
import polars as pl 
from typing import Dict, Any, Union, List

import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s-%(levelname)s-%(message)s')
logger= logging.getLogger(__name__)

class DistributionData: 
    def __init__(self, frame: pl.DataFrame, columns: List[str], analysis_config: Dict[str, Any]):
        self.frame= frame
        self.analysis_config= analysis_config
        self.columns= columns
    
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
        
        # LOGGER HERE
        
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
                
                if self.analysis_config.output.save_plots: 
                    dict_data[col]['plots']= str(self.save_plot(fig=fig, name=f'distribution_{col}.html'))
            elif col in cat: 
                dict_data[col]= self.cat_insights(col=col)
            else: 
                continue
        
        insights['distribution']=dict_data
        return dict_data

class OutliersData: 
    def __init__(self, frame: pl.DataFrame, columns: List[str], analysis_config: Dict[str, Any]):
        self.frame= frame
        self.columns= columns
        self.analysis_config= analysis_config
    
    def graph(self, col: str, outlier_frame: pl.DataFrame, n_out: int): 
        return px.scatter(
            x=outlier_frame[col].to_list(), 
            y=[0] * n_out, 
            title=f'Outliers_in_{col}'
        )
    
    def save_plot(self, fig, name: str) -> None: 
        folder= self.analysis_config.output.report_folder_name
        path= folder / name
        fig.write_html(path, include_plotlyjs="cdn")
        return path
    
    def iqr_method(self, col: str) -> List[Any]: 
        q1= self.frame[col].quantile(0.25)
        q3= self.frame[col].quantile(0.75)
        iqr= q3 - q1 
        lower= q1 - 1.5 * iqr
        upper= q3 + 1.5 * iqr
        outliers= self.frame.filter((pl.col(col) < lower) | (pl.col(col) > upper))
        n_out= len(outliers)
        pct_out= (n_out / len(self.frame)) * 100
        
        return [outliers, n_out, pct_out]
    
    def run_outlier(self, estrategy: str='iqr'): 
        insights= {}
        dict_data= {}
        
        for col in self.columns: 
            if estrategy == 'iqr':
                frame_out, n_out, pct_out= self.iqr_method(col=col)
            else: 
                continue #no more strategies available
            
            if n_out > 0: 
                fig= self.graph(col=col, outlier_frame=frame_out, n_out=n_out)
                
                # LOGGER HERE
                
                if self.analysis_config.output.save_plots: 
                    self.save_plot(fig=fig, name=f'outlier_{col}.html')
                else: 
                    continue
        return None

class CorrelationData: 
    def __init__(self, frame: pl.DataFrame, correlation: Dict[str, Any], configs: Dict[str, Any]):
        self.frame= frame
        self.correlation= correlation
        self.configs= configs
    
    def graph(self, corr: pl.DataFrame): 
        return px.imshow(
            corr, 
            text_auto=True, 
            color_continuous_scale=self.configs.plots.color_palette, 
            template=self.configs.plots.plotly_template, 
            title='Correlation Matrix'
        )
    
    def save_plot(self, fig, name: str) -> None: 
        folder= self.analysis_config.output.report_folder_name
        path= folder / name
        fig.write_html(path, include_plotlyjs="cdn")
        return path
    
    def run_corr(self): 
        columns= self.correlation.get('correlation')['columns']
        corr= self.frame.select(columns).corr()
        fig= self.graph(corr=corr)
        
        if self.configs.output.save_plots: 
            self.save_plot(fig=fig, name='correlation_matrix.html')
        
        # LOGGER

class CategoryDominance: 
    def __init__(self, frame: pl.DataFrame, category: Dict[str, Any], config: Dict[str, Any]):
        self.frame= frame
        self.category= category.get('category_dominance')
        self.config= config
    
    def graph(self, top: pl.DataFrame, col: str, n_bins: int): 
        return px.histogram(
            top, 
            x=col, 
            nbins=n_bins, 
            title=f'Distribution: {col}'
        )
    
    def save_plot(self, fig, name: str) -> None: 
        folder= self.analysis_config.output.report_folder_name
        path= folder / name
        fig.write_html(path, include_plotlyjs="cdn")
        return path
    
    def categorical(self): 
        columns= self.category['columns']
        top_n= self.category['top_n']
        rare_threshold= self.category['rare_threshold']
        n_bins= self.config.plots.histogram_bins
        
        for col in columns: 
            top= self.frame[col].value_count().sort('counts', descending=True).limit(top_n)
            fig= self.graph(top=top, col=col, n_bins=n_bins)
            
            if self.config.output.save_plots: 
                self.save_plot(fig=fig, name=f'distribution_{col}.html')
            
            #Rare catefories
            total= self.frame.height
            rare_count= self.frame[col].value_count().filter(pl.col('counts') < total*rare_threshold).height
            if rare_count > 0: 
                
                #LOGGER HERE
                
                pass


class AnalsisData: 
    def __init__(self, frame: pl.DataFrame, data_analysis: Dict[str, Any], analysis_config: Dict[str, Any]):
        self.frame= frame
        self.analysis_config= analysis_config
        self.data_analysis= data_analysis