import polars as pl 

class eda: 
    def __init__(self, frame: pl.DataFrame, null_threshold: float):
        self.frame= frame
        self.null_threshold= null_threshold
    
    def frame_head(self) -> None: 
        print('-- Frame Visualization --')
        print(self.frame.head(10))
    
    def shape(self) -> None: 
        print('-- Frame Shape --')
        
        total_rows, total_columns = self.frame.shape
        print(f'columns: {total_columns}, rows: {total_rows}')
    
    def existing_columns(self) -> None: 
        print('-- Existing Columns --')
        schema= self.frame.schema
        
        for col, tipo in schema.items(): 
            print(f'Column: {col}\nDatatype: {tipo}')
    
    def basic_descriptive_statistics(self) -> None: 
        print('-- Basic Descriptive Statistics --')
        print(self.frame.describe())
    
    def null_values(self) -> None: 
        print('-- Total Null Values --')
        nulls= self.frame.null_count()
        print(f'Total nulls: {nulls}')
        
        columnas= self.frame.columns
        null_values_detected= None
        
        for col in columnas: 
            null_percent= self.frame.select([
                (pl.col(col).null_count() / pl.col(col).len())
            ]).item()
            
            if null_percent > self.null_threshold: 
                if null_values_detected is None: 
                    print('\n-- High Null Density Detected --')
                    null_values_detected= True
                
                print(f'Warning: "{col}" has {null_percent:.2%}, which exceeds {self.null_threshold:.2%}')
    
    def run_eda(self) -> None: 
        print('-- Basic EDA --\n')
        self.frame_head()
        print()
        self.shape()
        print()
        self.existing_columns()
        print()
        self.basic_descriptive_statistics()
        print()
        self.null_values()



