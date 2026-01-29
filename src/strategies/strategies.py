from enum import Enum

class plotly_templates_strategy(str, Enum): 
    PLOTLY= 'plotly'
    PLOTLY_WHITE= 'plotly_white'
    PLOTLY_DARK= 'plotly_dark'
    GGPLOT2= 'ggplot2'
    SEABORN= 'seaborn'

class encoding_strategy(str, Enum): 
    UTF8= 'utf-8'
    ASCII= 'ascii'
    LATIN1= 'latin-1'


