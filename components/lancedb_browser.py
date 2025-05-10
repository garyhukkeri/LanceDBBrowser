import streamlit as st
import pandas as pd
import os

# Import the separated tab components
from components.db_connection import (
    is_running_in_docker, 
    connect, 
    connect_to_lancedb, 
    list_lancedb_tables
)
from components.table_browser import display_table_browser
from components.semantic_search import run_semantic_search_interface
from components.create_table import display_create_table_tab

def lancedb_browser():
    """
    Component for browsing and interacting with LanceDB tables.
    This is a standalone component that functions as a mini workbench
    for browsing and exploring LanceDB tables.
    """
    # Initialize session state variables
    if 'lancedb_connected' not in st.session_state:
        st.session_state.lancedb_connected = False
    if 'lancedb_connection' not in st.session_state:
        st.session_state.lancedb_connection = None
    if 'lancedb_tables' not in st.session_state:
        st.session_state.lancedb_tables = []
    if 'current_table' not in st.session_state:
        st.session_state.current_table = None
    
    # Check if running in Docker and automatically connect if host DB is configured
    if is_running_in_docker() and not st.session_state.lancedb_connected:
        host_db_path = os.environ.get('HOST_DB_PATH')
        if host_db_path:
            try:
                with st.spinner(f"Connecting to host database at {host_db_path}..."):
                    db = connect(host_db_path)
                    st.session_state.lancedb_connection = db
                    st.session_state.lancedb_connected = True
                    
                    # List available tables
                    table_names = list_lancedb_tables(db)
                    st.session_state.lancedb_tables = table_names
                    
                    st.success(f"Connected to LanceDB at {host_db_path}")
            except Exception as e:
                st.error(f"Error connecting to host database: {str(e)}")
    
    # Connection panel - only show if not running in Docker or if Docker connection failed
    if not (is_running_in_docker() and st.session_state.lancedb_connected):
        with st.expander("LanceDB Connection", expanded=not st.session_state.lancedb_connected):
            connect_to_lancedb()
    
    # If connected, show semantic search and then table browser
    if st.session_state.lancedb_connected and st.session_state.lancedb_connection:
        # Add tabs for different functionalities - Create Table is the last tab
        tab1, tab2, tab3 = st.tabs(["Table Browser", "Semantic Search", "Create Table"])
        
        # Get the db connection
        db = st.session_state.lancedb_connection
        
        # Display each tab's content
        with tab1:
            display_table_browser(db, list_lancedb_tables)
        
        with tab2:
            run_semantic_search_interface(db)
            
        with tab3:
            display_create_table_tab(db, list_lancedb_tables)