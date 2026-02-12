import plotly.express as px
import plotly.graph_objects as go

import polars as pl 
from typing import Dict, Any, Union, List, Optional
from pathlib import Path
from datetime import datetime
import json
import logging
import numpy as np

logging.basicConfig(level=logging.INFO, format='%(asctime)s-%(levelname)s-%(message)s')
logger= logging.getLogger(__name__)

class FoldersAndFiles: 
    def __init__(self, config: Dict[str, Any]):
        self.report_folder_name= config.report_folder_name
        self.insights_json_name= config.insights_json_name
    
    def date_folder(self) -> Path: 
        ruta= Path(__file__).resolve().parent.parent.parent
        now= datetime.now().strftime('%Y-%m-%d')
        
        folder_analysis= f'analysis_{now}'
        path= ruta / 'analysis_report' / folder_analysis
        path.mkdir(parents=True, exist_ok=True) 
        
        return path
    
    def analysis_folder(self,analysis: str) -> str: 
        ruta= self.date_folder()
        nueva_ruta= ruta / analysis
        nueva_ruta.mkdir(parents=True, exist_ok=True)
        return nueva_ruta
    
    def save_plot(self, fig: go.Figure, analysis: str, name: str) -> str:
        if fig is None: 
            return 'No plot found'
        folder_name= self.analysis_folder(analysis=analysis)
        path= self.report_folder_name / folder_name / name
        fig.write_html(path, include_plotlyjs="cdn")
        return path
    
    def json_save(self, dict_ins: Dict[str, Any], encoding: str='utf-8') -> Path: 
        resolve= Path(__file__).resolve().parent.parent.parent 
        path= resolve / 'analysis_report' / 'json_analysis' 
        file= path / self.insights_json_name
        
        try:
            with open(file, 'w', encoding=encoding) as f: 
                json.dump(dict_ins, f, indent=4, ensure_ascii=False)
            logger.info(f'Json report was written succesfully')
            return file
        except Exception as e: 
            logger.error(f'An error occurred while trying to write the JSON report {self.insights_json_name}. Error:\n{e}')
            raise ValueError(f'An error occurred while trying to write the JSON report {self.insights_json_name}. Error:\n{e}')

class Plots: 
    def __init__(self, config: Dict[str, Any]):
        self.n_bins= config.histogram_bins
        self.color_palet= config.color_palette
        self.template= config.plotly_template
    
    def distribution_plot(self, frame: pl.DataFrame, col: str) -> go.Figure: 
        logger.info(f'Histogram plot for column "{col}" was created')
        return px.histogram(
            frame, 
            x=col, 
            nbins=self.n_bins, 
            title=f'Distribution: {col}'
        )
    
    def bar(self, frame: pl.DataFrame, col: str) -> go.Figure: 
        logger.info(f'Bar plot for column "{col}" was created')
        return px.bar(
            frame, 
            x=col, 
            y='count', 
            template=self.template, 
            title=f'Top categories in "{col}"'
        )
    
    def outlier_plot(self, frame: pl.DataFrame, col: str) -> go.Figure: 
        logger.info(f'Scatter plot for column "{col}" was created')
        return px.box(
            frame, 
            x=col,
            title=f'Outliers_in_{col}'
        )
    
    def correlation_plot(self, frame: pl.DataFrame) -> go.Figure: 
        logger.info('The image of the correlation columns was created')
        return px.imshow(
            frame, 
            text_auto=True, 
            color_continuous_scale=self.color_palet, 
            template=self.template, 
            title='Correlation Matrix'
        )

class JsonSaveInsights: 
    @staticmethod
    def distribution_num_data(col: str, stat: pl.DataFrame, median: float, mean: float, skew: str) -> Dict[str, Any]:
        std= float(stat.filter(pl.col('statistic')=='std')['value'].item()) 
        q= [
            float(stat.filter(pl.col('statistic')=='25%')['value'].item()), 
            float(stat.filter(pl.col('statistic')=='75%')['value'].item())]
        min_v= float(stat.filter(pl.col('statistic')=='min')['value'].item())
        max_v= float(stat.filter(pl.col('statistic')=='max')['value'].item())
        
        num_data= {
            'col': col, 
            'mean': mean, 
            'median': median,
            'std': std,
            'iqr_25_75': q, 
            'min': min_v, 
            'max': max_v, 
            'skew': skew
        }
        
        logger.info(f'The insights from the data for the numeric column {col} have been obtained correctly')
        return num_data
    
    @staticmethod
    def distribution_cat_data(col: str, n_unique: int) -> Dict[str, Any]: 
        cat_insights= {
            'col': col,
            'type': 'categorical', 
            'unique_count': n_unique
        }
        logger.info(f'The insights from the data for the categoric column {col} have been obtained correctly')
        return cat_insights
    
    @staticmethod
    def outlier_data(col: str, n_out: int, pct_out: float, plot_bool: bool) -> Dict[str, Any]: 
        out_ins={
            'col': col,
            'plot': plot_bool,
            'total_outliers': n_out, 
            'percent_outliers': pct_out
        }
        logger.info(f'The insights from the outlier data for the column {col} have been obtained correctly')
        return out_ins
    
    @staticmethod
    def corr_data(cols: List[str], col_a: str, col_b: str, r_val: Union[str, float], plot_bool: bool) -> Dict[str, Any]: 
        corr_ins={
            'columns': cols, 
            'plot_bool': plot_bool,
            'top_correlation_a': col_a, 
            'top_correlation_b': col_b, 
            'r_value': r_val
        }
        logger.info(f'The insights from the correlation data for the columns {cols} have been obtained correctly')
        return corr_ins
    
    @staticmethod
    def categorical_data(col: str, top_labels: List[str], rare_count: int, rare_threshold: float) -> Dict[str, Any]: 
        cat_data={
            'col': col,
            'top_labs': top_labels, 
            'rare_count': rare_count, 
            'rare_threshold': rare_threshold
        }
        logger.info(f'The insights from the categorical data for the column {col} have been obtained correctly')
        return cat_data

class OperationAnalysis: 
    def __init__(self, frame: pl.DataFrame):
        self.frame= frame
    
    def describe_data(self, col: str) -> List[Union[float, pl.DataFrame, str]]: 
        stat= self.frame[col].describe()
        
        mean= float(stat.filter(pl.col('statistic')=='mean')['value'].item())
        median= float(stat.filter(pl.col('statistic')=='50%')['value'].item())
        
        skew= 'positive' if mean > median else 'negative' if mean < median else 'simetrico'
        
        logger.info(f'The describe of numeric column {col} was obtained correctly')
        return [mean, median, stat, skew]
    
    def unique_val(self, col: str) -> Union[int, pl.DataFrame]: 
        n= self.frame[col].n_unique()
        frame= self.frame[col].value_counts().sort('count', descending=True)
        logger.info(f'The unique data value for column {col} was obtained correctly')
        return [n, frame]
    
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
    
    def corr_op(self, columns: List[str]) -> List[Union[str, int, float, pl.DataFrame]]: 
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
        
        if (r_val == 0) or (r_val == None) or (np.isnan(r_val)): 
            logger.warning(f"The columns {columns} have zero standard deviation or is zero")
            r_val= f'The columns {columns} have zero standard deviation or is zero'
        
        logger.info(f'Correlation operation for columns {columns} were obtained correctly. "col_a", "col_b" and "r_val" were obtained.')
        return [col_a, col_b, r_val, corr]
    
    def category_op(self, col: str, top_n: int, rare_threshold: float) -> List[Any]: 
        top_count= self.frame[col].value_counts()
        total= self.frame.height
        
        top= top_count.sort('count', descending=True).limit(top_n)
        rare_count= top_count.filter(pl.col('count') < total*rare_threshold).height
        top_labels= [row[0] for row in top.rows()]
        
        logger.info(f'Categories dominance and rare categories for column {col} were obtained correctly')
        return [top, rare_count, top_labels]

class AnalysisData: 
    def __init__(self, frame: pl.DataFrame, config: Dict[str, Any]):
        self.frame= frame
        self.encoding= config.data.encoding
        
        self.analysis_config= config.analysis_config
        self.data_analysis= config.data_analysis
        
        self.folder= FoldersAndFiles(config=self.analysis_config.output)
        self.plots= Plots(config=self.analysis_config.plots)
        self.operation= OperationAnalysis(frame=self.frame)
        #Analysis
        self.in_dict= {}
    
    def dict_list_fill(self, 
            col: Union[str, List[str]], 
            analysis: str, 
            insight: Union[List[str], str], 
            plot: Union[go.Figure, str], 
            json: Dict[str, Any]) -> Optional[bool]: 
        save_plots= self.analysis_config.output.save_plots
        self.save_insights= self.analysis_config.output.save_insights
        auto_insights= self.data_analysis.auto_insights
        
        num=0
        
        if auto_insights: 
            insight
        else: 
            logger.warning(f'The insight for the column {col} was not generated in the console. It was not generated because auto_insights is False')
            num+=1
        
        if not plot: 
            save_plots= False
            str_path= plot
        
        if save_plots:
            if isinstance(col, str): 
                str_path= self.folder.save_plot(fig=plot, analysis=analysis, name=f'{analysis}_{col}.html')
            elif isinstance(col, list): 
                str_path= self.folder.save_plot(fig=plot, analysis=analysis, name=f'correlation_matrix.html')
            else: 
                str_path= None
        else: 
            logger.warning(f'The plot for the column {col} was not saved because save_plots is False')
            num+=1
        
        if self.save_insights:
            if analysis not in self.in_dict: 
                self.in_dict[analysis]= {}
            
            if isinstance(col, str): 
                self.in_dict[analysis][col]= json
                self.in_dict[analysis][col][f'analysis_path_{col}']= str(str_path) if str_path else None
            elif isinstance(col, list): 
                self.in_dict[analysis]['correlation']= json 
                self.in_dict[analysis]['correlation']['analysis_path']= str(str_path) if str_path else None
        else: 
            logger.warning(f'The insights for column {col} were not saved because save_insights is False')
            num+=1
        
        if num==3: 
            logger.warning(f'No {analysis} analysis is available for the data')
            return True
    
    def distribution(self, distribution_dic: Dict[str, Any]) -> None: 
        enable= distribution_dic.get('enable')
        columns= distribution_dic.get('columns')
        
        num= [col for col in self.frame.select(pl.selectors.numeric()).columns]
        cat= [col for col in self.frame.select(pl.selectors.string()).columns]
        
        if not enable: 
            logger.info('Distribution analysis is not enable')
            return 
        
        for col in columns:
            if col in num: 
                mean, median, stat_frame, skew= self.operation.describe_data(col=col)
                insight= logger.info(f'{col}: {skew} (media={median:.2f})')
                plots= self.plots.distribution_plot(frame=self.frame, col=col)
                json_save= JsonSaveInsights.distribution_num_data(
                    col=col, 
                    stat=stat_frame, 
                    median=median, 
                    mean=mean, 
                    skew=skew
                )
                r= self.dict_list_fill(
                    col=col, 
                    analysis='distribution', 
                    insight=insight, 
                    plot=plots, 
                    json=json_save
                )
                if r: 
                    break
            elif col in cat: 
                unique, frame= self.operation.unique_val(col=col)
                insight= logger.info(f'{unique} values for the column {col}')
                plots= self.plots.bar(
                    frame=frame, 
                    col=col
                )
                json_save= JsonSaveInsights.distribution_cat_data(col=col, n_unique=unique)
                r= self.dict_list_fill(
                    col=col, 
                    analysis='distribution', 
                    insight=insight, 
                    plot=plots, 
                    json=json_save
                )
                if r: 
                    break
            else: 
                logger.warning(f'Column "{col}" is not a numeric or categoric column')
    
    def outliers(self, outliers_dic: Dict[str, Any]) -> None: 
        enable= outliers_dic.get('enable')
        method= outliers_dic.get('method')
        columns= outliers_dic.get('columns')
        
        if not enable: 
            logger.info('Outliers analysis is not enable')
            return 
        
        for col in columns: 
            if method == 'iqr': 
                frame, n_out, pct_out= self.operation.iqr_method(col=col)
                insight= logger.info(f'{col}: {pct_out:.2f}% outliers')
                if n_out == 0: 
                    plots= False
                    plot_bool= False
                    logger.warning(f'The number of outliers is {n_out}, so it is not necessary to make a boxplot for column {col}')
                else: 
                    plots= self.plots.outlier_plot(frame=frame, col=col)
                    plot_bool= True
                json_save= JsonSaveInsights.outlier_data(col=col, n_out=n_out, pct_out=pct_out, plot_bool=plot_bool)
                r= self.dict_list_fill(
                    col=col, 
                    analysis='outliers', 
                    insight=insight, 
                    plot=plots, 
                    json=json_save
                )
                if r: 
                    break
            else: 
                logger.error(f'No method {method} available')
                raise
    
    def correlation(self, correlation_dic: Dict[str, Any]) -> None:
        enable= correlation_dic.get('enable')
        columns= correlation_dic.get('columns')
        
        if not enable: 
            logger.info('Correlation analysis is not enable')
            return None
        
        col_a, col_b, r_val, frame_corr= self.operation.corr_op(columns=columns)
        
        insight= logger.info(f'Top correlation A: {col_a}, top correlation B: {r_val}, r_value: {r_val}')
        if isinstance(r_val, str):
            plots= False
            plot_bool= False
            logger.warning(f'The correlation value is invalid. Correlation detected: {r_val}')
        else: 
            plots= self.plots.correlation_plot(frame=frame_corr)
            plot_bool= True
        json_save= JsonSaveInsights.corr_data(
            cols=columns, 
            col_a=col_a, 
            col_b=col_b, 
            r_val=r_val, 
            plot_bool=plot_bool
        )
        self.dict_list_fill(
            col=columns, 
            analysis='correlation', 
            insight=insight, 
            plot=plots, 
            json=json_save
        )
    
    def category_dominance(self, category_dom_dict: Dict[str, Any]) -> None: 
        enable= category_dom_dict.get('enable')
        top_n= category_dom_dict.get('top_n')
        rare_threshold= category_dom_dict.get('rare_threshold')
        columns= category_dom_dict.get('columns')
        
        if not enable: 
            logger.info('Categories analysis is not enable')
            return None
        
        for col in columns: 
            frame_top, rare_n, top_labels= self.operation.category_op(
                col=col, 
                top_n=top_n, 
                rare_threshold=rare_threshold 
            )
            insight= [
                logger.info(f'{col}: top=\n{frame_top}'), 
                logger.info(f'{col}: {rare_n} total rare categories')
            ]
            json_save= JsonSaveInsights.categorical_data(
                col=col, 
                top_labels=top_labels, 
                rare_count=rare_n, 
                rare_threshold=rare_threshold
            )
            r= self.dict_list_fill(
                col=col, 
                analysis='CategoryDominance', 
                insight=insight, 
                plot=None, 
                json=json_save
            )
            if r:
                break
    
    def run_analysis(self) -> Optional[Path]: 
        ins_q= self.data_analysis.insight_questions
        
        for i in range(len(ins_q)): 
            ins= ins_q[i].get('id')
            if ins == 'distribution': 
                self.distribution(distribution_dic=ins_q[i])
            elif ins == 'outliers': 
                self.outliers(outliers_dic=ins_q[i])
            elif ins == 'correlation': 
                self.correlation(correlation_dic=ins_q[i])
            elif ins == 'category_dominance': 
                self.category_dominance(category_dom_dict=ins_q[i])
            else: 
                logger.error(f'{ins} analysis doesnt exist')
                raise ValueError(f'{ins} analysis doesnt exist')
        
        if self.save_insights:
            return self.folder.json_save(dict_ins=self.in_dict, encoding=self.encoding)

