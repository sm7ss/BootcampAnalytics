import streamlit as st
from src.streamlit.dynamic_functions import FrameOperations, Streategies, Filter, FilterResults
from main import data
from datetime import datetime

frame, file= data()
data_name= f'{file.name}'

frame_data= FrameOperations(frame=frame)
frame_filtered= FilterResults(frame=frame)

st.set_page_config(
    page_title="DataAnalysis",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title('📊 Data Analysis - Interactive')
st.divider()

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
    
    st.subheader('🗂️ Group By')
    available_to_group= [v for v in frame_data.available_columns() if v != cols]
    groups= st.multiselect(
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
    if 'filters' not in st.session_state: 
        st.session_state['filters']= []
    
    if 'filter_status' not in st.session_state:
        st.session_state['filter_status']= []
    
    if 'filter_operations' not in st.session_state: 
        st.session_state['filter_operations']= []
    
    num_cols= frame_data.numeric_columns()
    cat_cols= frame_data.categoric_columns()
    
    filter_class= Filter(frame=frame)
    
    add, remove= st.columns(2)
    with add: 
        if st.button('➕ Add filter'): 
            st.session_state.filters.append(
                f'Filter {len(st.session_state.filters) + 1}'
            )
    
    for i, filter_i in enumerate(st.session_state.filters): 
        n= 0
        st.divider()
        st.write(f'📌 {filter_i}')
        
        col= st.selectbox(
            f'Column {i+1}', 
            frame_data.available_columns(), 
            key=f'col_{i}'
        )
        
        if col in num_cols: 
            min_val= frame[col].min()
            max_val= frame[col].max()
            
            personalize= st.checkbox('Personalize filter', key=f'personalize_{i}', value=True)
            
            if personalize: 
                dict_hist= filter_class.filter_numeric_operator(
                    col=col, 
                    i=i, 
                    min_val=min_val, 
                    max_val=max_val
                )
            else: 
                dict_hist= filter_class.filter_numeric_slider(
                    col=col, 
                    i=i, 
                    max_val=max_val, 
                    min_val=min_val, 
                )
        elif col in cat_cols: 
            unique_val= frame[col].drop_nans().drop_nulls().unique().to_list()
            
            dict_hist= filter_class.filter_categoric(
                col=col, 
                i=i, 
                unique_val=unique_val
            )
        else: 
            st.write('Not numeric or categoric')
        
        if dict_hist and dict_hist.get('value'):
            status= dict_hist.copy()
            status['timestamp']=datetime.now().strftime('%H:%M:%S')
            st.session_state.filter_status.append(status)
        
        if dict_hist and dict_hist.get('value') and dict_hist not in st.session_state.filter_operations: 
            st.session_state.filter_operations.append(dict_hist)
        
        if st.button('Delete filter', key=f'delete_{i}'): 
            try: 
                st.session_state.filter_operations.pop()
                st.session_state.filters.pop()
                st.rerun()
            except Exception as e: 
                st.error('There are no active filters')
    
    if st.session_state.filter_status: 
        st.markdown('📜 Filter history')
        for i, state in enumerate(st.session_state.filter_status): 
            if state.get('value'): 
                st.info(f"""
                    📌 Column: {state.get('col')}  \nType: {state.get('type')}  \nValue: {state.get('value')}  \nTime: {state.get('timestamp')}
                """)
        
        if st.button(f'🗞 Clear History Filter', key=f'clean_filter'): 
            st.session_state.filter_status = []
            st.rerun()

with st.expander('🧮 Value Column Sample', expanded=True): 
    st.caption(f'Sample of 10 rows from the Dataframe of {file.name}')
    st.dataframe(frame.head(10), width='stretch')

with st.expander('📊 Operation Result', expanded=True): 
    result_op= frame_data.operation_result(col=cols, operator=op)
    st.write(result_op)

with st.expander(f'🗂️ Group By {cols.capitalize()}', expanded=True): 
    if groups: 
        grouped= frame_data.group_by_result(groups=groups, col=cols, operator=op)
        st.caption(f'Grouped by: {groups}')
        st.dataframe(grouped.head(1000), width='stretch')
    else: 
        st.info(f'Select a column in "Group By" to see the preview of {cols}')

with st.expander('🎨 Visualization', expanded=True): 
    st.caption(f'Type: {vis}')
    if vis: 
        if vis == 'heatmap':
            st.caption('Select a numeric column. If no numeric column is selected, all numeric columns will be taken.')
            
            columns_heat_map= st.multiselect(
                'Numeric Columns', 
                num_cols.columns, 
                label_visibility='collapsed'
            )
            
            if len(columns_heat_map) == 1: 
                st.info(f'At least two columns must be selected in order to calculate the correlation.')
            elif len(columns_heat_map) > 1: 
                figure= frame_data.visualization_filter(vis=vis, col=columns_heat_map)
                st.plotly_chart(figure, width='stretch', height=700)
            else: 
                figure= frame_data.visualization_filter(vis=vis, col=num_cols.columns)
                st.plotly_chart(figure, width='stretch',  height=700)
        else:
            if groups: 
                use_group_frame= st.checkbox('Apply grouping frame')
                if use_group_frame: 
                    figure= frame_data.visualization_filter(vis=vis, col=cols, frame_grouped=grouped)
                    st.plotly_chart(figure, width='stretch')
                else: 
                    figure= frame_data.visualization_filter(vis=vis, col=cols)
                    st.plotly_chart(figure, width='stretch')
            else: 
                figure= frame_data.visualization_filter(vis=vis, col=cols)
                st.plotly_chart(figure, width='stretch')
    else: 
        st.info(f'Select a visualization plot in "Visualization" to see the plot for {cols}')

with st.expander(f'🔍 Filter {cols.capitalize()}', expanded=True): 
    if st.session_state.filter_operations: 
        i= 0
        for dict_expr in st.session_state.filter_operations: 
            i+= 1
            ff=frame_filtered.operation_filter_result_run(dict_info=dict_expr)
            percent= ff.height/frame.height
            st.write(f'Data available after filtering: {ff.height}/{frame.height} filas ({percent:.2%})')
            st.dataframe(ff)
            if st.button('Delete filter', key=f'delete_{i}_values'): 
                try: 
                    st.session_state.filter_operations.pop()
                    st.session_state.filters.pop()
                    st.rerun()
                except Exception as e: 
                    st.error('There are no active filters')
                    st.rerun()
            st.divider()
    else: 
        st.info('No filters available. Apply one or more filters.')

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
