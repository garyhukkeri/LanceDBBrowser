"""
Streamlit Adapter Module

This module adapts the core LanceDB services and components to the Streamlit UI framework.
"""
import streamlit as st
import pandas as pd
from typing import Optional, Dict, Any
from services.lancedb_service import LanceDBService
from services.embedding_service import EmbeddingService
from services.semantic_search_service import SemanticSearchService
from services.table_operations_service import TableOperationsService
from utils.error_utils import handle_error, AppError
from utils.env_utils import is_running_in_docker, get_default_db_path
import logging

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class StreamlitLanceDBAdapter:
    """
    Adapter class that connects LanceDB services with Streamlit UI.
    Handles UI state management and user interactions.
    """
    
    def __init__(self):
        self._initialize_services()
        self._initialize_session_state()
        
    def _initialize_services(self):
        """Initialize service instances."""
        # Create or retrieve services from session state
        if 'lancedb_service' not in st.session_state:
            st.session_state.lancedb_service = LanceDBService()
        if 'embedding_service' not in st.session_state:
            st.session_state.embedding_service = EmbeddingService()
            
        self.db_service = st.session_state.lancedb_service
        self.embedding_service = st.session_state.embedding_service
        
        # Initialize dependent services
        self.semantic_search = SemanticSearchService(
            db_service=self.db_service,
            embedding_service=self.embedding_service
        )
        self.table_ops = TableOperationsService(
            db_service=self.db_service
        )
        
    def _initialize_session_state(self):
        """Initialize Streamlit session state variables."""
        defaults = {
            'lancedb_connected': False,
            'lancedb_tables': [],
            'current_table': None,
            'data': None,
            'error': None
        }
        
        for key, value in defaults.items():
            if key not in st.session_state:
                st.session_state[key] = value
    
    def _handle_error(self, error: Exception) -> None:
        """Handle and display errors in the UI."""
        error_data = handle_error(error)
        error_msg = error_data['error']['message']
        
        if isinstance(error, AppError):
            st.error(error_msg)
            if error_data['error']['details']:
                st.json(error_data['error']['details'])
        else:
            st.error(f"Unexpected error: {error_msg}")
            logger.error("Unexpected error", exc_info=True)
    
    def handle_connection(self):
        """Handle database connection in the UI."""
        try:
            # Auto-connect in Docker environment
            if is_running_in_docker() and not st.session_state.lancedb_connected:
                self._handle_docker_connection()
                
            # Show connection panel if needed
            if not st.session_state.lancedb_connected:
                with st.expander("LanceDB Connection", expanded=True):
                    self._display_connection_form()
                    
        except Exception as e:
            self._handle_error(e)
    
    def _handle_docker_connection(self):
        """Handle automatic connection in Docker environment."""
        host_db_path = get_default_db_path()
        if host_db_path:
            with st.spinner(f"Connecting to database at {host_db_path}..."):
                if self.db_service.connect(host_db_path):
                    st.session_state.lancedb_connected = True
                    self._refresh_table_list()
                    st.success(f"Connected to LanceDB at {host_db_path}")
    
    def _display_connection_form(self):
        """Display the connection form in the UI."""
        db_path = st.text_input(
            "LanceDB Path",
            value=get_default_db_path(),
            placeholder="/path/to/lancedb"
        )
        
        if st.button("Connect"):
            with st.spinner("Connecting to LanceDB..."):
                try:
                    if self.db_service.connect(db_path):
                        st.session_state.lancedb_connected = True
                        self._refresh_table_list()
                        st.success(f"Connected to LanceDB at {db_path}")
                except Exception as e:
                    self._handle_error(e)
    
    def _refresh_table_list(self):
        """Refresh the list of available tables."""
        try:
            tables = self.table_ops.list_tables()
            st.session_state.lancedb_tables = tables.get('data', [])
        except Exception as e:
            self._handle_error(e)
    
    def display_table_browser(self):
        """Display the table browser interface."""
        if not st.session_state.lancedb_connected:
            st.warning("Please connect to a LanceDB database first.")
            return
            
        try:
            st.subheader("Browse Tables")
            
            col1, col2 = st.columns([1, 3])
            
            with col1:
                self._display_table_list()
                
            with col2:
                if st.session_state.current_table:
                    self._display_table_details(st.session_state.current_table)
                    
        except Exception as e:
            self._handle_error(e)
    
    def _display_table_list(self):
        """Display the list of available tables."""
        st.write("Available Tables:")
        
        if st.button("Refresh Tables"):
            with st.spinner("Refreshing table list..."):
                self._refresh_table_list()
                st.success(f"Found {len(st.session_state.lancedb_tables)} tables")
                st.rerun()
        
        if st.session_state.lancedb_tables:
            selected_table = st.radio(
                "Select Table",
                st.session_state.lancedb_tables,
                label_visibility="collapsed"
            )
            
            if selected_table != st.session_state.current_table:
                st.session_state.current_table = selected_table
                st.rerun()
        else:
            st.info("No tables found. Create a table or refresh the list.")
    
    def _display_table_details(self, table_name: str):
        """Display details and data for a selected table."""
        try:
            st.subheader(f"Table: {table_name}")
            
            tab1, tab2, tab3 = st.tabs(["Preview Data", "Table Schema", "Execute Query"])
            
            with tab1:
                self._display_table_preview(table_name)
                
            with tab2:
                self._display_table_schema(table_name)
                
            with tab3:
                self._display_query_interface(table_name)
                
        except Exception as e:
            self._handle_error(e)
    
    def _display_table_preview(self, table_name: str):
        """Display a preview of table data."""
        limit = st.slider("Number of rows to preview", 5, 100, 10)
        
        with st.spinner("Loading data preview..."):
            result = self.table_ops.get_table_data(table_name, limit)
            if result['success']:
                data = result['data']
                st.write(f"Showing {data['row_count']} rows")
                st.dataframe(data['data'])
            else:
                self._handle_error(AppError(result['error']['message']))
    
    def _display_table_schema(self, table_name: str):
        """Display table schema information."""
        try:
            schema = self.table_ops.get_table_schema(table_name)
            if schema['success']:
                st.json(schema['data'])
            else:
                self._handle_error(AppError(schema['error']['message']))
        except Exception as e:
            self._handle_error(e)
    
    def _display_query_interface(self, table_name: str):
        """Display the query interface for a table."""
        try:
            result = self.table_ops.get_non_vector_columns(table_name)
            if not result['success']:
                st.error("Failed to get table columns")
                return
                
            columns = result['data']
            if not columns:
                st.info("No queryable columns found in this table")
                return
                
            col = st.selectbox("Select column to filter", columns)
            op = st.selectbox("Select operator", ["equals", "contains", "greater than", "less than"])
            value = st.text_input("Enter filter value")
            
            if st.button("Execute Query"):
                self._execute_table_query(table_name, col, op, value)
                
        except Exception as e:
            self._handle_error(e)
    
    def _execute_table_query(self, table_name: str, col: str, op: str, value: str):
        """Execute a query on a table."""
        try:
            # Implementation of query execution
            pass
        except Exception as e:
            self._handle_error(e)
    
    def display_semantic_search(self):
        """Display the semantic search interface."""
        if not st.session_state.lancedb_connected:
            st.warning("Please connect to a LanceDB database first.")
            return
            
        try:
            st.subheader("Semantic Search")
            
            # Get tables with embeddings
            embedding_tables = self.semantic_search.get_embedding_tables()
            if not embedding_tables['success']:
                self._handle_error(AppError(embedding_tables['error']['message']))
                return
                
            if not embedding_tables['data']:
                st.info("No tables with embedding columns found.")
                return
                
            # Display search interface
            table_name, embedding_col = st.selectbox(
                "Select table",
                embedding_tables['data'],
                format_func=lambda x: f"{x[0]} (embedding: {x[1]})"
            )
            
            # Get available models
            models = self.semantic_search.get_available_models()
            if not models['success']:
                self._handle_error(AppError(models['error']['message']))
                return
                
            model_name = st.selectbox(
                "Select model",
                list(models['data'].keys()),
                format_func=lambda x: f"{x} ({models['data'][x]['description']})"
            )
            
            query = st.text_area("Enter search query")
            limit = st.slider("Number of results", 1, 50, 10)
            
            if st.button("Search") and query:
                self._perform_semantic_search(
                    table_name, query, embedding_col, model_name, limit
                )
                
        except Exception as e:
            self._handle_error(e)
    
    def _perform_semantic_search(self, table_name: str, query: str,
                               embedding_col: str, model_name: str, limit: int):
        """Perform semantic search and display results."""
        try:
            with st.spinner("Performing search..."):
                results = self.semantic_search.search_by_text(
                    table_name=table_name,
                    query_text=query,
                    embedding_column=embedding_col,
                    model_name=model_name,
                    limit=limit
                )
                
                if results['success']:
                    processed = self.semantic_search.process_search_results(
                        results['data']
                    )
                    if processed['success']:
                        st.write(f"Found {processed['data']['total_results']} results:")
                        st.dataframe(pd.DataFrame(processed['data']['results']))
                    else:
                        self._handle_error(AppError(processed['error']['message']))
                else:
                    self._handle_error(AppError(results['error']['message']))
                    
        except Exception as e:
            self._handle_error(e)
    
    def display_create_table(self):
        """Display the table creation interface."""
        if not st.session_state.lancedb_connected:
            st.warning("Please connect to a LanceDB database first.")
            return
            
        try:
            st.subheader("Create Table")
            
            create_type = st.radio(
                "Creation method",
                ["Upload Data", "Create Sample Table"]
            )
            
            if create_type == "Upload Data":
                self._display_upload_interface()
            else:
                self._display_sample_table_interface()
                
        except Exception as e:
            self._handle_error(e)
    
    def _display_upload_interface(self):
        """Display interface for uploading data."""
        table_name = st.text_input("Table name")
        uploaded_file = st.file_uploader("Choose a file", type=['csv', 'parquet'])
        
        if uploaded_file and table_name:
            if st.button("Create Table"):
                with st.spinner("Creating table..."):
                    try:
                        result = self.table_ops.create_table(
                            table_name=table_name,
                            data=uploaded_file
                        )
                        
                        if result['success']:
                            st.success(f"Table '{table_name}' created successfully!")
                            self._refresh_table_list()
                        else:
                            self._handle_error(AppError(result['error']['message']))
                            
                    except Exception as e:
                        self._handle_error(e)
    
    def _display_sample_table_interface(self):
        """Display interface for creating sample tables."""
        table_name = st.text_input("Table name")
        columns = st.text_input("Column names (comma-separated)")
        
        if table_name and columns:
            col_list = [c.strip() for c in columns.split(",")]
            if st.button("Create Sample Table"):
                with st.spinner("Creating sample table..."):
                    try:
                        result = self.table_ops.create_sample_table(
                            table_name=table_name,
                            columns=col_list
                        )
                        
                        if result['success']:
                            st.success(f"Sample table '{table_name}' created successfully!")
                            self._refresh_table_list()
                        else:
                            self._handle_error(AppError(result['error']['message']))
                            
                    except Exception as e:
                        self._handle_error(e)
    
    def run_browser_interface(self):
        """Run the main browser interface."""
        st.title("LanceDB Browser")
        
        # Handle connection first
        self.handle_connection()
        
        # Show main interface if connected
        if st.session_state.lancedb_connected:
            tab1, tab2, tab3 = st.tabs([
                "Browse Tables",
                "Semantic Search",
                "Create Table"
            ])
            
            with tab1:
                self.display_table_browser()
                
            with tab2:
                self.display_semantic_search()
                
            with tab3:
                self.display_create_table()
