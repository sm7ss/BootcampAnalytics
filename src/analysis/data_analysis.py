import plotly.express as px
import polars as pl 
from typing import Dict, Any, Union, List, Callable
from pathlib import Path
from datetime import datetime

import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s-%(levelname)s-%(message)s')
logger= logging.getLogger(__name__)

# Auto-insight para mostrar, y save insights si config=True

class DistributionData: 
    def __init__(self, frame: pl.DataFrame, distribution: Dict[str, Any], config: Dict[str, Any]):
        self.frame= frame
        self.config= config
        self.distribution= distribution
    
    def plot(self, col: str, n_bins: int): 
        
        return px.histogram(
            self.frame, 
            x=col, 
            nbins=n_bins, 
            title=f'Distribution: {col}'
        )
    
    def describe_data(self, col: str) -> List[float]: 
        stat= self.frame[col].describe()
        
        mean= float(stat.filter(pl.col('statistic')=='mean')['value'].item())
        median= float(stat.filter(pl.col('statistic')=='50%')['value'].item())
        std= float(stat.filter(pl.col('statistic')=='std')['value'].item())
        
        return [mean, median, std, stat]
    
    def save_json_file_num_data(self, col: str) -> Dict[str, Any]:
        mean,median, std, stat= self.describe_data(col=col)
        q5, q95= stat.quantile(0.05), self.frame[col].quantile(0.95)
        
        num_data= {
            'type': 'numeric', 
            'mean': mean, 
            'median': median,
            'std': std,
            'iqr_5_95': [float(q5), float(q95)], 
            'min': float(stat.filter(pl.col('statistic')=='min')['value'].item()), 
            'max': float(stat.filter(pl.col('statistic')=='max')['value'].item())
        }
        
        logger.info(f'The insights from the data for the numeric column {col} have been obtained correctly')
        return num_data
    
    def save_json_file_cat_data(self, col: str) -> Dict[str, Any]: 
        cat_insights= {
            'type': 'categorical', 
            'unique_count': self.frame[col].n_unique()
        }
        
        logger.info(f'The insights from the data for the categoric column {col} have been obtained correctly')
        return cat_insights
    
    def insights_num_data(self, col: str) -> str: 
        mean, median, std= self.describe_data(col=col)
        
        skew= 'positive' if mean > median else 'negative' if mean < median else 'simetrico'
        
        logger.info(f'The insight for the column {col} was created')
        logger.info(f'{col}: {skew} (media={median:.2f})')
        return f'- {col}: media={mean:.0f}, median={median:.2f}, std={std:.2f} -> sesgo {skew}'
    
    def insights_cat_data(self, col: str) -> str: 
        unique= self.frame[col].n_unique()
        
        logger.info(f'The unique data value for column {col} was obtained correctly')
        logger.info(f'{unique} values for the column {col}')
        return f'{col} unique values= {unique}'
    
    def run_distribution(self, save_plot: Callable, auto_insights: bool= False) -> Union[Dict[str, Any], List[str]]: 
        num= [col for col in self.frame.select(pl.selectors.numeric()).columns]
        cat= [col for col in self.frame.select(pl.selectors.string()).columns]
        
        n_bins= self.config.plots.histogram_bins
        save_plots= self.config.output.save_plots
        
        if save_plots: 
            ruta= Path(__file__).resolve().parent.parent.parent
            now= datetime.now().strftime('%Y-%m-%d')
            path= ruta / f'distribution_{now}'
            path.mkdir(parents=True, exist_ok=True) 
        
        save_insights= self.config.output.save_insights
        columns= self.distribution.get('columns')
        
        insights= []
        json_file= {}
        
        for col in columns: 
            if col in num: 
                if auto_insights: 
                    self.insights_num_data(col=col)
                else: 
                    logger.warning(f'The insight for column {col} was not generated in the console. It was not generated because auto_insights is False')
                
                if save_insights: 
                    ins= self.insights_num_data(col=col)
                    insights.append(ins)
                else: 
                    logger.warning(f'The insights for column {col} were not saved because save_insights is False')
                
                if save_plots: 
                    json_file[col]= self.save_json_file_num_data(col=col)
                    fig= self.plot(col=col, n_bins=n_bins)
                    json_file[col]['plots']= str(save_plot(fig=fig, name=f'distribution_{col}.html'))
                else: 
                    logger.warning(f'The plot for the column {col} was not saved because save_plots is False')
            elif col in cat: 
                if auto_insights: 
                    self.insights_cat_data(col=col)
                else: 
                    logger.warning(f'The insight for column {col} was not generated in the console. It was not generated because auto_insights is False')
                
                if save_insights: 
                    ins= self.insights_cat_data(col=col)
                    insights.append(ins)
                else: 
                    logger.warning(f'The insights for column {col} were not saved because save_insights is False')
                
                if save_plots: 
                    json_file[col]= self.save_json_file_cat_data(col=col)
                else: 
                    logger.warning(f'The plot for the column {col} was not saved because save_plots is False')
            else: 
                logger.warning(f'The column {col} is not a numerical or categorical column')
                continue
        
        return [json_file, insights]

'''save_plots= self.config.output.save_plots
        
        if save_plots: 
            ruta= Path(__file__).resolve().parent.parent.parent
            now= datetime.now().strftime('%Y-%m-%d')
            path= ruta / f'distribution_{now}'
            path.mkdir(parents=True, exist_ok=True) 
        
        save_insights= self.config.output.save_insights
        columns= self.distribution.get('columns')'''

class OutliersData: 
    def __init__(self, frame: pl.DataFrame, outliers: Dict[str, Any], config: Dict[str, Any]):
        self.frame= frame
        self.outliers= outliers
        self.config= config
    
    def plot(self, col: str, outlier_frame: pl.DataFrame, n_out: int): 
        return px.scatter(
            x=outlier_frame[col].to_list(), 
            y=[0] * n_out, 
            title=f'Outliers_in_{col}'
        )
    
    def iqr_method(self, col: str) -> List[Union[pl.DataFrame, int, float]]: 
        q1= self.frame[col].quantile(0.25)
        q3= self.frame[col].quantile(0.75)
        iqr= q3 - q1 
        lower= q1 - 1.5 * iqr
        upper= q3 + 1.5 * iqr
        outliers= self.frame.filter((pl.col(col) < lower) | (pl.col(col) > upper))
        n_out= len(outliers)
        pct_out= (n_out / len(self.frame)) * 100
        
        logger.info(f'The frame with outliers (if applicable), the number of filtered outliers, and the percentage of total outliers for column {col} were obtained correctly')
        return [outliers, n_out, pct_out]
    
    def insights(col: str, n_out: int, pct_out: float) -> str: 
        logger.info(f'{col}: {pct_out:.2f}% outliers')
        return f'- {col}: {n_out} outliers ({pct_out:.2f}%)'
    
    def run_outlier(self, save_plot: Callable, auto_insights: bool= False) -> Union[Dict[str, Any], List[str]]: 
        columns= self.outliers.get('columns')
        method= self.outliers.get('method')
        
        insight= []
        dict_data= {}
        
        save_plots= self.config.output.save_plots
        save_insights= self.config.output.save_insights
        
        if save_plots: 
            ruta= Path(__file__).resolve().parent.parent.parent
            now= datetime.now().strftime('%Y-%m-%d')
            path= ruta / f'outliers_{now}'
            path.mkdir(parents=True, exist_ok=True) 
        
        for col in columns: 
            if method == 'iqr':
                frame_out, n_out, pct_out= self.iqr_method(col=col)
            else: 
                logger.warning('There are no more methods available for outliers')
                continue #no more strategies available
            
            if n_out > 0: 
                if auto_insights: 
                    self.insights(col=col, n_out=n_out, pct_out= pct_out)
                else: 
                    logger.warning(f'The insight for column {col} was not generated in the console. It was not generated because auto_insights is False')
                
                if save_insights: 
                    ins= self.insights(col=col, n_out=n_out, pct_out= pct_out)
                    insight.append(ins)
                else: 
                    logger.warning(f'The insights for column {col} were not saved because save_insights is False')
                
                if save_plots: 
                    dict_data[col]={
                        'total_outliers': n_out, 
                        'percent_outliers': pct_out, 
                        'frame_sample': frame_out.limit(10)
                    }
                    fig= self.plot(col=col, outlier_frame=frame_out, n_out=n_out)
                    dict_data[col]['plots']= str(save_plot(fig=fig, name=f'outlier_{col}.html'))
                else: 
                    logger.warning(f'The plot for the column {col} was not saved because save_plots is False')
            else: 
                logger.info(f'{col}: without outliers')
                continue
        
        return [dict_data, insight]

class CorrelationData: 
    def __init__(self, frame: pl.DataFrame, correlation: Dict[str, Any], configs: Dict[str, Any]):
        self.frame= frame
        self.correlation= correlation
        self.configs= configs
    
    def plot(self, corr: pl.DataFrame): 
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
    
    def corr_op(self) -> List[Union[str, int, float, pl.DataFrame]]: 
        columns= self.correlation.get('columns')
        corr= self.frame.select(columns).corr()
        
        corr_ins= corr.with_columns(column=pl.Series(corr.columns))
        melted= corr_ins.unpivot(index='column')
        
        top_corr= (
            melted
            .filter(pl.col('column') != pl.col('variable'))
            .with_columns(abs_value=pl.col('value').abs())
            .sort('abs_value', descending=True)
            .limit(1)
        )
        
        col_a= top_corr['column'][0]
        col_b= top_corr['variable'][0]
        r_val= top_corr['value'][0]
        
        return [columns, col_a, col_b, r_val, corr]
    
    def run_corr(self, save_plot: Callable, auto_insights: bool= False) -> Union[Dict[str, Any], List[str]]:
        cols, col_a, col_b, r_val, corr= self.corr_op()
        
        insights= []
        json_file= {}
        
        save_plots= self.configs.output.save_plots
        save_insights= self.configs.output.save_insights
        
        if save_plots: 
            ruta= Path(__file__).resolve().parent.parent.parent
            now= datetime.now().strftime('%Y-%m-%d')
            path= ruta / f'correlation_{now}'
            path.mkdir(parents=True, exist_ok=True) 
        
        if auto_insights: 
            logger.info(f'Top correlation A: {col_a}, top correlation B: {r_val}, r_value: {r_val}')
        else: 
            logger.warning(f'The insight for columns {cols} was not generated in the console. It was not generated because auto_insights is False')
        
        if save_insights: 
            logger.info(f'Top correlation A: {col_a}, top correlation B: {r_val}, r_value: {r_val}')
            insights.append(f"- Strongest correlation: {col_a} vs {col_b} (r={r_val:.2f})")
        else: 
            logger.warning(f'The insights for columns {cols} were not saved because save_insights is False')
        
        if save_plots: 
            json_file['num_columns']= {
                'columns': cols, 
                'top_correlation_a': col_a, 
                'top_correlation_b': col_b, 
                'r_value': r_val
            }
            fig= self.plot(corr=corr)
            json_file['num_columns']['plots']= str(save_plot(fig=fig, name=f'correlation.html'))
        else: 
            logger.warning(f'The plot for the columns {cols} was not saved because save_plots is False')
        
        return [json_file, insights]

class CategoryDominance: 
    def __init__(self, frame: pl.DataFrame, category: Dict[str, Any], config: Dict[str, Any]):
        self.frame= frame
        self.category= category.get('category_dominance')
        self.config= config
    
    def plot(self, top: pl.DataFrame, col: str, n_bins: int): 
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
    
    
    
    def categorical(self, save_plot: Callable, auto_insights: bool= False) -> Union[Dict[str, Any], List[str]]: 
        columns= self.category.get('columns')
        top_n= self.category.get('top_n')
        rare_threshold= self.category.get('rare_threshold')
        
        insights= []
        json_field= {}
        
        n_bins= self.config.plots.histogram_bins
        save_plots= self.config.output.save_plots
        save_insights= self.config.output.save_insights
        
        for col in columns: 
            top= self.frame[col].value_count().sort('counts', descending=True).limit(top_n)
            fig= self.plot(top=top, col=col, n_bins=n_bins)
            
            if self.config.output.save_plots: 
                save_plot(fig=fig, name=f'distribution_{col}.html')
            
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


print(path.exists())