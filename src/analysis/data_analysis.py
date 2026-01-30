import plotly.express as px
import polars as pl 
from typing import Dict, Any, Union, List

class AnalsisData: 
    def __init__(self, frame: pl.DataFrame, plots: Dict[str, Any], output: Dict[str, Any], representative_columns: Union[str, List[str]]=None):
        self.frame= frame
        self.plots= plots
        self.output= output
        self.representative_columns= representative_columns
    
    def clean_nulls(self) -> pl.DataFrame: 
        new_frame= self.frame.select(self.representative_columns)
        return new_frame
    