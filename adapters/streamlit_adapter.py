"""
Streamlit Adapter Module

This module adapts the core LanceDB service to the Streamlit UI framework.
It handles all UI-specific logic while delegating business operations to the service.
"""
import streamlit as st
import pandas as pd
import os
from services.lancedb_service import LanceDBService
from utils.env_utils import is_running_in_docker, get_default_db_path
from components.semantic_search import run_semantic_search_interface


class StreamlitLanceDBAdapter:
    """
    Adapter class that connects the LanceDBService with Streamlit UI components.
    This class handles UI state management and delegates business operations to the service.
    """
    
    def __init__(self):
        self._initialize_session_state()
        
        # Create or retrieve the service from session state
        if 'lancedb_service' not in st.session_state:
            st.session_state.lancedb_service = LanceDBService()
        
        # Use the service from session state
        self.service = st.session_state.lancedb_service
    
    def _initialize_session_state(self):
        """Initialize the Streamlit session state variables."""
        if 'lancedb_connected' not in st.session_state:
            st.session_state.lancedb_connected = False
        if 'lancedb_tables' not in st.session_state:
            st.session_state.lancedb_tables = []
        if 'current_table' not in st.session_state:
            st.session_state.current_table = None
        if 'data' not in st.session_state:
            st.session_state.data = None
    
    def handle_connection(self):
        """Handle LanceDB connection in the Streamlit interface."""
        # Check if running in Docker and auto-connect
        if is_running_in_docker() and not st.session_state.lancedb_connected:
            host_db_path = os.environ.get('HOST_DB_PATH')
            if host_db_path:
                try:
                    with st.spinner(f"Connecting to host database at {host_db_path}..."):
                        self.service.connect(host_db_path)
                        st.session_state.lancedb_connected = True
                        
                        # List available tables
                        table_names = self.service.list_tables()
                        st.session_state.lancedb_tables = table_names
                        
                        st.success(f"Connected to LanceDB at {host_db_path}")
                except Exception as e:
                    st.error(f"Error connecting to host database: {str(e)}")
        
        # Connection panel if needed
        if not (is_running_in_docker() and st.session_state.lancedb_connected):
            with st.expander("LanceDB Connection", expanded=not st.session_state.lancedb_connected):
                self._display_connection_form()
    
    def _display_connection_form(self):
        """Display connection form in the UI."""
        default_path = get_default_db_path()
        
        db_path = st.text_input("LanceDB Path", 
                              value=default_path,
                              placeholder="/path/to/lancedb")
        
        if st.button("Connect"):
            try:
                with st.spinner("Connecting to LanceDB..."):
                    self.service.connect(db_path)
                    st.session_state.lancedb_connected = True
                    
                    # List available tables
                    table_names = self.service.list_tables()
                    st.session_state.lancedb_tables = table_names
                    
                    st.success(f"Connected to LanceDB at {db_path}")
            except Exception as e:
                st.error(f"Error connecting to LanceDB: {str(e)}")
    
    def display_table_browser(self):
        """Display the table browser interface."""
        if not st.session_state.lancedb_connected:
            st.warning("Please connect to a LanceDB database first.")
            return
        
        # Verify connection is still valid before proceeding
        if not self.service.ensure_connection():
            st.session_state.lancedb_connected = False
            st.error("Database connection was lost. Please reconnect.")
            return
            
        try:
            # Get list of tables
            tables = st.session_state.lancedb_tables
            
            if not tables:
                st.info("No tables found in the database.")
                return
                
            # Table selection
            selected_table = st.selectbox(
                "Select a table to browse",
                options=tables,
                index=0 if tables else None
            )
            
            if selected_table:
                st.session_state.current_table = selected_table
                
                # Query limit
                query_limit = st.number_input("Result limit", 
                                            min_value=1, 
                                            max_value=1000, 
                                            value=100)
                
                if st.button("Load Data"):
                    with st.spinner(f"Loading data from '{selected_table}'..."):
                        # Double check connection before query
                        if not self.service.ensure_connection():
                            st.session_state.lancedb_connected = False
                            st.error("Database connection was lost. Please reconnect.")
                            return
                            
                        # Query the table and store in session state
                        result_df = self.service.query_table(
                            selected_table, 
                            limit=query_limit
                        )
                        st.session_state.data = result_df
                        
                        # Display statistics and preview
                        st.success(f"Loaded {len(result_df)} rows from '{selected_table}'")
                
                # Display data if available
                if st.session_state.data is not None:
                    st.dataframe(st.session_state.data)
                        
        except Exception as e:
            st.error(f"Error browsing tables: {str(e)}")
    
    def display_semantic_search(self):
        """Display semantic search interface."""
        if not st.session_state.lancedb_connected:
            st.warning("Please connect to a LanceDB database first.")
            return
            
        # Verify connection is still valid
        if not self.service.ensure_connection():
            st.session_state.lancedb_connected = False
            st.error("Database connection was lost. Please reconnect.")
            return
            
        try:
            # Get list of tables
            tables = st.session_state.lancedb_tables
            
            if not tables:
                st.info("No tables found in the database.")
                return
                
            # Table selection for search
            selected_table = st.selectbox(
                "Select a table for semantic search",
                options=tables,
                index=0 if tables else None,
                key="semantic_search_table"
            )
            
            if selected_table:
                # Get the database connection from the service
                db_connection = self.service.get_connection()
                # Call the semantic search interface with the database connection
                run_semantic_search_interface(db_connection)
                
        except Exception as e:
            st.error(f"Error in semantic search: {str(e)}")
    
    def display_create_table(self):
        """Display interface for creating new tables."""
        if not st.session_state.lancedb_connected:
            st.warning("Please connect to a LanceDB database first.")
            return
            
        # Verify connection is still valid
        if not self.service.ensure_connection():
            st.session_state.lancedb_connected = False
            st.error("Database connection was lost. Please reconnect.")
            return
            
        try:
            st.subheader("Create New Table")
            
            # Table name input
            new_table_name = st.text_input("New Table Name", key="new_table_name")
            
            # File upload
            uploaded_file = st.file_uploader(
                "Upload CSV or Parquet file", 
                type=["csv", "parquet"]
            )
            
            if uploaded_file and new_table_name:
                if st.button("Create Table"):
                    with st.spinner("Creating table..."):
                        # Read the uploaded file
                        if uploaded_file.name.endswith('.csv'):
                            df = pd.read_csv(uploaded_file)
                        else:
                            df = pd.read_parquet(uploaded_file)
                        
                        # Create the table using the service
                        self.service.create_table(new_table_name, df)
                        
                        # Update the list of tables
                        st.session_state.lancedb_tables = self.service.list_tables()
                        
                        st.success(f"Table '{new_table_name}' created successfully!")
                        
        except Exception as e:
            st.error(f"Error creating table: {str(e)}")
    
    def run_browser_interface(self):
        """Run the main LanceDB browser interface."""
        # Handle connection first
        self.handle_connection()
        
        # If connected, show the tabs
        if st.session_state.lancedb_connected:
            tab1, tab2, tab3 = st.tabs(["Table Browser", "Semantic Search", "Create Table"])
            
            with tab1:
                self.display_table_browser()
            
            with tab2:
                self.display_semantic_search()
                
            with tab3:
                self.display_create_table()
