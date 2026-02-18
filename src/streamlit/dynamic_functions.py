from ..strategies.app_strategies import operations_strategies_cat, operations_strategies_num, visualization_strategies_num, visualization_strategies_cat
import polars as pl
from typing import List, Union
import streamlit as st

class FrameOperations: 
    def __init__(self, frame: pl.DataFrame):
        self.frame= frame
    
    def available_columns(self) -> List[str]: 
        return self.frame.columns
    
    def numeric_columns(self) -> List[str]: 
        return self.frame.select(pl.selectors.numeric())
    
    def categoric_columns(self) -> List[str]: 
        return self.frame.select(pl.selectors.string())

class Streategies:
    @staticmethod
    def operations_num() -> List[str]: 
        return [v.value.capitalize() for v in operations_strategies_num]
    
    @staticmethod
    def operations_cat() -> List[str]: 
        return [v.value for v in operations_strategies_cat]
    
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
                return range_slider
        else: 
            write= st.number_input(f'Input numeric value for {col}', key=f'input_numeric_{i}')
            if write: 
                if st.button(f'Apply filter', key=f'button_{i}'): 
                    if write < min_val or write > max_val:
                        st.error(f'❎ Range out of values. Range >= {min_val} <= {max_val}')
                        return None
                    else: 
                        st.success(f'✅ Filter in column {col} between {min_val} and {max_val}')
                        return write
    
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
                    'operator': operator, 
                    'value': val
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
                    return search_value
        else: 
            selection= st.multiselect(
                f'Select values in column {col}', 
                unique_val, 
                key=f'selection_unique_{i}'
            )
            if st.button(f'Apply filter', key=f'button_{i}'):
                st.success('✅ Filter was applied')
                return selection





