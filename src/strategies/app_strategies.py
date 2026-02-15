from enum import Enum

class operations_strategies_num(str, Enum): 
    SUM= 'sum'
    AVG= 'avg'
    MAX= 'max'
    MIN= 'min'
    COUNT= 'count'

class operations_strategies_cat(str, Enum): 
    COUNT= 'count'
    UNIQUE= 'unique'

class visualization_strategies_num(str, Enum): 
    HISTOGRAM= 'histogram'
    BOXPLOT= 'boxplot'
    HEATMAP= 'heatmap'

class visualization_strategies_cat(str, Enum): 
    HISTOGRAM= 'histogram'






