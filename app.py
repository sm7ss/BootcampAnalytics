import streamlit as st
from src.streamlit.dynamic_functions import columns
from main import data

frame, file= data()
data_name= f'{file.name}'
column= columns.available_columns(frame=frame)

st.set_page_config(
    page_title="DataAnalysis",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title('📊 Data Analysis - Interactive')

with st.sidebar: 
    st.title('📊 Analysis Sidebar')
    st.divider()
    dark_mode= st.sidebar.toggle("🌓 Dark Mode", value=False)
    st.divider()
    
    st.subheader('📂 Data Source')
    st.write(f'{data_name}')
    
    st.subheader('🧮 Value Column')
    cols= st.selectbox(
        'Column to analyze', 
        column, 
        label_visibility='collapsed'
    )
    
    st.subheader('📊 Operation')
    op= st.radio(
        'Operation', 
        columns.operations(), 
        label_visibility='collapsed'
    )
    
    groups= st.subheader('🗂️ Group By')
    st.multiselect(
        'Group by', 
        column, 
        label_visibility='collapsed'
    )
    
    st.subheader('🎨 Visualization')
    vis= st.selectbox(
        'plot type', 
        columns.visualization(), 
        label_visibility='collapsed'
    )
    
    st.subheader('🔍 Filters')
    with st.expander('Filters', expanded=True): 
        pass
    

if dark_mode: 
    bg_color = "#0E1117"
    text_color = "#FAFAFA"
    card_bg = "#1E1E1E"
    border_color = "#333333"
    sidebar_bg = "#1A1C24"
    header_bg = "#0E1117" 
else: 
    bg_color = "#FFFFFF"
    text_color = "#31333F"
    card_bg = "#F8F9FA"
    border_color = "#E0E0E0"
    sidebar_bg = "#E8EAEEE9"
    header_bg = "#FFFFFF"

st.markdown(f"""
<style>
    .stApp {{
        background-color: {bg_color};
        color: {text_color} !important;
    }}
    
    section[data-testid="stSidebar"] {{
        background-color: {sidebar_bg} !important;
    }}
    
    section[data-testid="stSidebar"] * {{
        color: {text_color} !important;
    }}
    
    header[data-testid="stHeader"] {{
        background-color: {header_bg} !important;
    }}
    
    header[data-testid="stHeader"] * {{
        color: {text_color} !important;
    }}
    
    .stApp header {{
        background-color: {header_bg} !important;
    }}
    
    button[kind="header"] {{
        background-color: transparent !important;
        color: {text_color} !important;
    }}
    
    .main {{
        background-color: {bg_color};
        color: {text_color};
    }}
    
    .main * {{
        color: {text_color};
    }}
    
    .metric-card {{
        background-color: {card_bg};
        border: 1px solid {border_color};
        border-radius: 10px;
        padding: 1rem;
        margin: 0.5rem 0;
        color: {text_color};
    }}
    
    .stSelectbox div[data-baseweb="select"] div {{
        background-color: {card_bg} !important;
        color: {text_color} !important;
        border-color: {border_color} !important;
    }}
    
    .stRadio div[role="radiogroup"] {{
        color: {text_color} !important;
    }}
    
    .stDataFrame {{
        background-color: {card_bg} !important;
        color: {text_color} !important;
    }}
    
    .streamlit-expanderHeader {{
        color: {text_color} !important;
        background-color: {card_bg} !important;
    }}
    
    hr {{
        border-color: {border_color} !important;
    }}
    
    h1, h2, h3 {{
        margin-top: 0rem !important;
        margin-bottom: 0rem !important;
    }}
    
    [data-testid="stSidebar"] [data-testid="stVerticalBlock"] > div {{
        padding-top: 0rem !important;
        padding-bottom: 0rem !important;
        margin-top: -0.5rem !important; 
    }}
    
    [data-testid="stSidebar"] hr {{
        margin-top: 0.5rem !important;
        margin-bottom:1.5rem !important;
    }}
    
    [data-testid="stSidebar"] h1, 
    [data-testid="stSidebar"] h2, 
    [data-testid="stSidebar"] h3 {{
        margin-top: 0rem !important;
        padding-top: 0rem !important;
        }}
</style>
""", unsafe_allow_html=True)

st.divider()



















