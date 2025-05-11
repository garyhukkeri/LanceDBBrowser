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

# # If data has been loaded from a LanceDB query
# if st.session_state.data is not None:
#     st.header("Current Dataset")
    # df = st.session_state.data
    
    # # Show data stats
    # st.write(f"Rows: {len(df)}, Columns: {len(df.columns)}")
    
    # # Show data preview
    # with st.expander("Data Preview", expanded=True):
    #     st.dataframe(df.head(10), use_container_width=True)
