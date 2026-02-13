from ..strategies.app_strategies import operations_strategies, visualization_strategies
import polars as pl
from typing import List

class columns: 
    def available_columns(frame: pl.DataFrame) -> List[str]: 
        return frame.columns
    
    def filters(frame: pl.DataFrame) : 
        pass
    
    def operations() -> List[str]: 
        return [v.value.capitalize() for v in operations_strategies]
    
    def visualization() -> List[str]: 
        return [v.value for v in visualization_strategies]








