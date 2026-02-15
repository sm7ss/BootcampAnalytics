from ..strategies.app_strategies import operations_strategies_cat, operations_strategies_num, visualization_strategies_num, visualization_strategies_cat
import polars as pl
from typing import List

class FrameOperations: 
    def __init__(self, frame: pl.DataFrame):
        self.frame= frame
    
    def available_columns(self) -> List[str]: 
        return self.frame.columns
    
    def numeric_columns(self) -> List[str]: 
        return self.frame.select(pl.selectors.numeric())
    
    def categoric_columns(self) -> List[str]: 
        return self.frame.select(pl.selectors.string())
    
    def filters(self) : 
        av_colums= self.available_columns()
        
        
        
        

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







