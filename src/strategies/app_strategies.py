from enum import Enum

class operations_strategies(str, Enum): 
    SUM= 'sum'
    AVG= 'avg'
    MAX= 'max'
    MIN= 'min'
    COUNT= 'count'
    UNIQUE= 'unique'

class visualization_strategies(str, Enum): 
    HISTOGRAM= 'histogram'
    BOXPLOT= 'boxplot'
    HEATMAP= 'heatmap'








