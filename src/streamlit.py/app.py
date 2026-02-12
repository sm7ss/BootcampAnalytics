import streamlit as st

st.title('Data Analysis 📊')

dark_mode= st.sidebar.toggle("🌓 Dark Mode", value=False)

if dark_mode: 
    bg_color= '#0E1117'
    text_color= '#FAFAFA'
    card_bg= '#1E1E1E'
    border_color= '#333333'
else: 
    bg_color = "#FFFFFF"
    text_color = "#31333F"
    card_bg = "#F8F9FA"
    border_color = "#E0E0E0"

st.markdown(f"""
<style>
    /* This targets the main background and text variables */
    :root {{
        --background-color: {bg_color};
        --secondary-background-color: {card_bg};
        --text-color: {text_color};
    }}

    .stApp {{
        background-color: var(--background-color);
    }}

    /* Fixes the sidebar background */
    [data-testid="stSidebar"] {{
        background-color: {card_bg};
    }}

    /* Custom card styling */
    .metric-card {{
        background-color: {card_bg};
        border: 1px solid {border_color};
        border-radius: 10px;
        padding: 1rem;
        color: {text_color};
    }}
</style>
""", unsafe_allow_html=True)















