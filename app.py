import streamlit as st
import pandas as pd
from adapters.streamlit_adapter import StreamlitLanceDBAdapter

# Set page configuration
st.set_page_config(
    page_title="LanceDB Browser",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state variables
if 'data' not in st.session_state:
    st.session_state.data = None

# Application title and description
st.title("LanceDB Browser")
st.caption("A mini workbench for browsing and exploring LanceDB tables")

# Initialize and run the adapter
adapter = StreamlitLanceDBAdapter()
adapter.run_browser_interface()