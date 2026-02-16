import streamlit as st
from src.streamlit.dynamic_functions import FrameOperations, Streategies
from main import data

frame, file= data()
data_name= f'{file.name}'

frame_data= FrameOperations(frame=frame)

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
        frame_data.available_columns(), 
        label_visibility='collapsed'
    )
    
    st.subheader('📊 Operation')
    if cols in frame_data.numeric_columns(): 
        op= st.radio(
            'Operation', 
            Streategies.operations_num(), 
            label_visibility='collapsed'
        )
    elif cols in frame_data.categoric_columns(): 
        op= st.radio(
            'Operation', 
            Streategies.operations_cat(), 
            label_visibility='collapsed'
        )
    else: 
        op= None
        st.write(f'No available visualization for column {cols}')
    
    groups= st.subheader('🗂️ Group By')
    available_to_group= [v for v in frame_data.available_columns() if v != cols]
    st.multiselect(
        'Group by', 
        available_to_group, 
        label_visibility='collapsed'
    )
    
    st.subheader('🎨 Visualization')
    if cols in frame_data.numeric_columns(): 
            vis= st.selectbox(
            'plot type', 
            Streategies.visualization_num(), 
            label_visibility='collapsed'
        )
    elif cols in frame_data.categoric_columns(): 
            vis= st.selectbox(
            'plot type', 
            Streategies.visualization_cat(), 
            label_visibility='collapsed'
        )
    else: 
        vis= None
        st.write(f'No available visualization for column {cols}')
    
    st.subheader('🔍 Filters')
    num_cols= frame_data.numeric_columns()
    cat_cols= frame_data.categoric_columns()
    
    with st.expander('Filters', expanded=True): 
        if cols in num_cols: 
            min_val= float(frame[cols].min()) 
            max_val= float(frame[cols].max())
            
            range_c= st.slider(
                f'Range {cols}', 
                min_value=min_val, 
                max_value=max_val, 
                value=(min_val, max_val), 
                key=f'Auto_filter_{cols}'
            )
            
            col1, col2= st.columns(2)
            with col1: 
                operator= st.selectbox(
                    'Operator', 
                    ['=', '>', '>=', '<', '<=', 'between'], 
                    key='filter_custom_op_1'
                )
            with col2: 
                if operator == 'between': 
                    min_val_op= st.number_input('Min', key='min_op')
                    max_val_op= st.number_input('Max', key='max_op')
                    val= (min_val_op, max_val_op)
                else: 
                    val= st.number_input(
                        'Value', 
                        key=f'filter_custom_op_2'
                    )
            
            if val: 
                if operator == 'between': 
                    if val[0] < min_val: 
                        st.error(f'❎ Min {val[0]} < {min_val}. Range out of data')
                    elif val[1] > max_val: 
                        st.error(f'❎ Max {val[1]} > {max_val}. Range out of data')
                    else: 
                        st.success(f'✅ Valid range')
                else: 
                    if val < min_val or val > max_val: 
                        st.error(f'❎ Range out of data. [{min_val}/{max_val}]')
                    else: 
                        st.success(f'✅ Valid range')
                        
        elif cols in cat_cols: 
            unique_values= frame[cols].drop_nulls().drop_nans().unique().to_list()
            
            selection= st.multiselect(
                f'Filter {cols}', 
                unique_values, 
                key=f'Auto_filter_{cols}'
            )
            
            st.markdown(f'**Custom filter {cols}**')
            search_v= st.text_input(
                f'Filter value in {cols}', 
                key= f'filter_custom_value_{cols}'
            ).strip()
            
            if search_v: 
                if search_v in unique_values: 
                    st.success(f'✅ "{search_v}" exist in data')
                    selection+= search_v
                else: 
                    st.error(f'❎ "{search_v}" doesnt exist in data')
            
        else: 
            pass
        
        st.markdown(f'**Custom filter {cols}**')
        
        if 'num_filters_custom' not in st.session_state: 
            st.session_state.num_filters_custom = 0
        
        col1_, col2_= st.sidebar.columns(2)
        with col1_: 
            if st.button('➕ Add filter'):
                st.session_state.num_filters_custom+= 1
        with col2_: 
            if st.button('➖ Remove filter'): 
                st.session_state.num_filters_custom-= 1
        
        for i in range(st.session_state.num_filters_custom): 
            st.sidebar.divider()
            
            column_cus= st.sidebar.selectbox(
                f'column per filter {i+1}', 
                frame.columns, 
                key=f'filter_cus_{cols}'
            )
        

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



















