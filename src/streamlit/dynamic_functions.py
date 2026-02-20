from ..strategies.app_strategies import operations_strategies_cat, operations_strategies_num, visualization_strategies_num, visualization_strategies_cat
import polars as pl
from typing import List, Union, Optional
import streamlit as st
import plotly.express as px

class FrameOperations: 
    def __init__(self, frame: pl.DataFrame):
        self.frame= frame
    
    def available_columns(self) -> List[str]: 
        return self.frame.columns
    
    def numeric_columns(self) -> List[str]: 
        return self.frame.select(pl.selectors.numeric())
    
    def categoric_columns(self) -> List[str]: 
        return self.frame.select(pl.selectors.string())
    
    def operation_result(self, col: str, operator: str) -> str: 
        num= self.numeric_columns()
        cat= self.categoric_columns()
        
        if col in num: 
            match operator.lower(): 
                case operations_strategies_num.SUM: 
                    result= self.frame[col].sum()
                case operations_strategies_num.AVG: 
                    result= self.frame[col].mean()
                case operations_strategies_num.MAX: 
                    result=  self.frame[col].max()
                case operations_strategies_num.MIN: 
                    result=  self.frame[col].min()
                case operations_strategies_num.COUNT: 
                    result= self.frame[col].count()
            result= float(result)
            return f'{operator} of {col}. Result = {result:,.2f}'
        elif col in cat: 
            match operator.lower(): 
                case operations_strategies_cat.UNIQUE: 
                    result= self.frame[col].n_unique()
                    top_count= self.frame[col].value_counts()
                    top= top_count.sort('count', descending=True).limit(10)
                    list_top= [row[0] for row in top.rows()]
                    
                    return f'{operator} of {col}. Result = {result}  \nTop Labels: {list_top}'
                case operations_strategies_cat.COUNT: 
                    result= self.frame[col].count()
                    return f'{operator} of {col}. Result = {result}'
        else: 
            return st.write(f'The column "{col}" is not a numeric or categoric file')
    
    def group_by_result(self, groups: List[str], col: str, operator: str) -> str: 
        operator= operator.lower()
        if operator == 'avg': 
            expresion= pl.col(col).mean()
        else: 
            expresion= getattr(pl.col(col), operator)()
        return self.frame.group_by(groups).agg(expresion)
    
    def visualization_filter(self, vis: str, col: str, frame_grouped: Optional[pl.DataFrame]=None): 
        match vis: 
            case visualization_strategies_num.HISTOGRAM: 
                return px.histogram(
                        self.frame if frame_grouped is None else frame_grouped, 
                        x=col, 
                        nbins=30,
                        title=f'Distribution: {col}'
                    )
            case visualization_strategies_num.BOXPLOT: 
                return px.box(
                    self.frame if frame_grouped is None else frame_grouped, 
                    x=col, 
                    title=f'Outliers_in_{col}'
                )
            case visualization_strategies_num.HEATMAP: 
                num= self.frame[col].drop_nulls()
                corr= num.corr()
                columns= corr.columns
                
                return px.imshow(
                    corr.to_numpy(),
                    x=columns, 
                    y=columns,  
                    text_auto=True, 
                    title='Correlation Matrix'
                )
            case visualization_strategies_cat.HISTOGRAM: 
                return px.histogram(
                        self.frame if None else frame_grouped, 
                        x=col, 
                        nbins=30,
                        title=f'Distribution: {col}'
                    )

class Streategies:
    @staticmethod
    def operations_num() -> List[str]: 
        return [v.value.capitalize() for v in operations_strategies_num]
    
    @staticmethod
    def operations_cat() -> List[str]: 
        return [v.value.capitalize() for v in operations_strategies_cat]
    
    @staticmethod
    def visualization_num() -> List[str]: 
        return [v.value for v in visualization_strategies_num]
    
    @staticmethod
    def visualization_cat() -> List[str]: 
        return [v.value for v in visualization_strategies_cat]

class Filter: 
    def __init__(self, frame: pl.DataFrame):
        self.frame= frame
    
    def filter_numeric_slider(self, col: str, i: int, min_val: Union[int, float], max_val: Union[int, float]): 
        slider= st.checkbox('Use range slider', key=f'use_slider_{i}')
        
        if slider: 
            range_slider= st.slider(
                f'Range {col}', 
                min_value=min_val, 
                max_value=max_val, 
                value=(min_val, max_val), 
                key=f'slider_{i}'
            )
            if st.button(f'Apply filter', key=f'button_{i}'): 
                st.success(f'✅ Filter in column {col} was applied')
                return {
                    'col': col, 
                    'type': 'numeric_slider', 
                    'value': range_slider
                }
        else: 
            write= st.number_input(f'Input numeric value for {col}', key=f'input_numeric_{i}')
            if write: 
                if st.button(f'Apply filter', key=f'button_{i}'): 
                    if write < min_val or write > max_val:
                        st.error(f'❎ Range out of values. Range >= {min_val} <= {max_val}')
                        return None
                    else: 
                        st.success(f'✅ Filter in column {col} between {min_val} and {max_val}')
                        return {
                            'col': col, 
                            'type': 'numeric_slider', 
                            'value': write
                        }
    
    def filter_numeric_operator(self, col: str, i: int, min_val: Union[int, float], max_val: Union[int, float]): 
        col_op, col_val= st.columns(2)
        
        with col_op: 
            operator= st.selectbox(
                'Operator', 
                ['>', '>=', '<', '<=', '='], 
                key=f'op_{i}'
            )
        with col_val: 
            val= st.number_input(
                'Value', 
                value=min_val, 
                key=f'val_{i}'
            )
        
        if st.button(f'Apply filter', key=f'button_{i}'): 
            if val < min_val or val > max_val: 
                st.error(f'❎ Value out of range. Range >= {min_val} <= {max_val}')
                return None
            else: 
                st.success(f'✅ Filter in column {col} was applied')
                return {
                    'col': col, 
                    'type': 'numeric_operator', 
                    'value': val, 
                    'operator': operator
                }
    
    def filter_categoric(self, col: str, i: int, unique_val: List[str]): 
        search= st.checkbox(f'Search values', key=f'search_{i}')
        
        if search: 
            search_value= st.text_input(f'Search value in column {col}', key=f'search_value_{i}')
            if st.button(f'Apply filter', key=f'button_{i}'): 
                if search_value not in unique_val: 
                    st.error(f'❎ Value {search_value} not found in column {col}')
                    return None
                else: 
                    st.success(f'✅ Filter in column {col} was applied')
                    return {
                        'col': col, 
                        'type': 'unique_categoric_value', 
                        'value': search_value
                    }
        else: 
            selection= st.multiselect(
                f'Select values in column {col}', 
                unique_val, 
                key=f'selection_unique_{i}'
            )
            if st.button(f'Apply filter', key=f'button_{i}'):
                st.success('✅ Filter was applied')
                return {
                        'col': col, 
                        'type': 'unique_categoric_value', 
                        'value': selection
                    }
