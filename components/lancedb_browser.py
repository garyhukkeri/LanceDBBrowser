import streamlit as st
import pandas as pd
import os
import json
from pathlib import Path

# Mock LanceDB implementation for the browser interface
class MockLanceDBTable:
    """Mock implementation of a LanceDB table for UI demonstration purposes."""
    
    def __init__(self, name, schema=None):
        self.name = name
        self._schema = schema or [
            {"name": "id", "type": "int32", "nullable": False},
            {"name": "name", "type": "string", "nullable": True},
            {"name": "embedding", "type": "float32[128]", "nullable": True}
        ]
        self._data = pd.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "name": ["Sample 1", "Sample 2", "Sample 3", "Sample 4", "Sample 5"],
            "embedding": ["[...]", "[...]", "[...]", "[...]", "[...]"]
        })
    
    def to_pandas(self, limit=10):
        """Return mock data as pandas DataFrame."""
        return self._data.head(limit)
    
    def count_rows(self):
        """Return mock row count."""
        return len(self._data)
    
    @property
    def schema(self):
        """Return mock schema."""
        class Field:
            def __init__(self, name, type, nullable):
                self.name = name
                self.type = type
                self.nullable = nullable
        
        # Convert 'type' field to avoid keyword collision
        schema_fields = []
        for field in self._schema:
            field_copy = field.copy()
            # No need to rename - we'll pass the arguments directly
            schema_fields.append(Field(
                name=field_copy["name"],
                type=field_copy["type"],
                nullable=field_copy["nullable"]
            ))
        
        return schema_fields
    
    def search(self, query_str):
        """Mock search function that returns the original data."""
        return self
        
    def delete_rows(self, indices):
        """Delete rows from the mock table by indices."""
        if not indices:
            return False
            
        # Convert to list if necessary
        if not isinstance(indices, list):
            indices = [indices]
            
        # Sort indices in descending order to avoid index shifting
        indices = sorted(indices, reverse=True)
        
        # Remove rows
        for idx in indices:
            if 0 <= idx < len(self._data):
                self._data = self._data.drop(self._data.index[idx])
                
        # Reset index
        self._data = self._data.reset_index(drop=True)
        return True
        

class MockLanceDB:
    """Mock implementation of LanceDB for UI demonstration purposes."""
    
    def __init__(self, uri):
        self.uri = uri
        self._tables = {
            "users": MockLanceDBTable("users", [
                {"name": "id", "type": "int32", "nullable": False},
                {"name": "name", "type": "string", "nullable": True},
                {"name": "age", "type": "int32", "nullable": True},
                {"name": "email", "type": "string", "nullable": True},
                {"name": "embedding", "type": "float32[128]", "nullable": True}
            ]),
            "products": MockLanceDBTable("products", [
                {"name": "id", "type": "int32", "nullable": False},
                {"name": "name", "type": "string", "nullable": True},
                {"name": "price", "type": "float32", "nullable": True},
                {"name": "category", "type": "string", "nullable": True},
                {"name": "embedding", "type": "float32[128]", "nullable": True}
            ]),
            "transactions": MockLanceDBTable("transactions", [
                {"name": "id", "type": "int32", "nullable": False},
                {"name": "user_id", "type": "int32", "nullable": False},
                {"name": "product_id", "type": "int32", "nullable": False},
                {"name": "amount", "type": "float32", "nullable": False},
                {"name": "timestamp", "type": "string", "nullable": False}
            ])
        }
    
    def table_names(self):
        """Return list of mock table names."""
        return list(self._tables.keys())
    
    def open_table(self, table_name):
        """Return a mock table by name."""
        if table_name in self._tables:
            return self._tables[table_name]
        raise ValueError(f"Table {table_name} not found")
    
    def create_table(self, table_name, data):
        """Create a mock table."""
        schema = []
        for col in data.columns:
            schema.append({
                "name": col,
                "type": "string",  # Default type
                "nullable": True
            })
        self._tables[table_name] = MockLanceDBTable(table_name, schema)
        return self._tables[table_name]

# Mock connection function to replace lancedb.connect
def connect(uri):
    """Mock connect function that returns a MockLanceDB instance."""
    return MockLanceDB(uri)

def lancedb_browser():
    """
    Component for browsing and interacting with LanceDB tables.
    This is a standalone component that functions as a mini workbench
    for browsing and exploring LanceDB tables.
    """
    st.header("LanceDB Browser")
    
    # Initialize session state variables
    if 'lancedb_connected' not in st.session_state:
        st.session_state.lancedb_connected = False
    if 'lancedb_connection' not in st.session_state:
        st.session_state.lancedb_connection = None
    if 'lancedb_tables' not in st.session_state:
        st.session_state.lancedb_tables = []
    if 'current_table' not in st.session_state:
        st.session_state.current_table = None
    
    # Connection panel
    if not st.session_state.lancedb_connected:
        st.subheader("LanceDB Connection")
        connect_to_lancedb()
    
    # If connected, show table browser
    if st.session_state.lancedb_connected and st.session_state.lancedb_connection:
        display_table_browser()


def connect_to_lancedb():
    """Handle connection to LanceDB database."""
    st.subheader("Connect to LanceDB")
    
    # LanceDB can connect to a local directory or URI
    connection_type = st.radio(
        "Connection Type",
        ["Local Directory", "URI Connection"],
        horizontal=True
    )
    
    if connection_type == "Local Directory":
        # Local directory connection
        db_path = st.text_input(
            "Database Path",
            value="./lancedb_data",
            help="Path to the local LanceDB database directory"
        )
        
        if st.button("Connect to Local Database"):
            try:
                with st.spinner("Connecting to LanceDB..."):
                    # Create directory if it doesn't exist
                    os.makedirs(db_path, exist_ok=True)
                    
                    # Connect to the database using our mock function
                    db = connect(db_path)
                    
                    # Store connection in session state
                    st.session_state.lancedb_connection = db
                    st.session_state.lancedb_connected = True
                    
                    # List available tables
                    table_names = list_lancedb_tables(db)
                    st.session_state.lancedb_tables = table_names
                    
                    st.success(f"Connected to LanceDB at {db_path}")
                    if table_names:
                        st.info(f"Found {len(table_names)} tables: {', '.join(table_names)}")
                    else:
                        st.info("No tables found in the database.")
            except Exception as e:
                st.error(f"Error connecting to LanceDB: {str(e)}")
    else:
        # URI connection (e.g., S3, etc.)
        uri = st.text_input(
            "Database URI",
            placeholder="s3://bucket-name/path/to/db",
            help="URI to the LanceDB database location"
        )
        
        # Additional credentials for cloud storage
        st.subheader("Connection Credentials")
        col1, col2 = st.columns(2)
        with col1:
            region = st.text_input("Region", placeholder="us-east-1")
            access_key = st.text_input("Access Key ID")
        with col2:
            secret_key = st.text_input("Secret Access Key", type="password")
        
        if st.button("Connect to Remote Database"):
            if not uri:
                st.error("Database URI is required")
            else:
                try:
                    with st.spinner("Connecting to LanceDB..."):
                        # Set AWS credentials if provided
                        if access_key and secret_key:
                            os.environ["AWS_ACCESS_KEY_ID"] = access_key
                            os.environ["AWS_SECRET_ACCESS_KEY"] = secret_key
                        
                        if region:
                            os.environ["AWS_REGION"] = region
                        
                        # Connect to the database using our mock function
                        db = connect(uri)
                        
                        # Store connection in session state
                        st.session_state.lancedb_connection = db
                        st.session_state.lancedb_connected = True
                        
                        # List available tables
                        table_names = list_lancedb_tables(db)
                        st.session_state.lancedb_tables = table_names
                        
                        st.success(f"Connected to LanceDB at {uri}")
                        if table_names:
                            st.info(f"Found {len(table_names)} tables: {', '.join(table_names)}")
                        else:
                            st.info("No tables found in the database.")
                except Exception as e:
                    st.error(f"Error connecting to LanceDB: {str(e)}")


def list_lancedb_tables(db):
    """
    Get list of tables from a LanceDB connection.
    
    Args:
        db: LanceDB connection object
        
    Returns:
        List of table names
    """
    try:
        return db.table_names()
    except Exception as e:
        st.error(f"Error listing tables: {str(e)}")
        return []


def display_table_browser():
    """Display the browser interface for LanceDB tables."""
    st.subheader("Browse Tables")
    
    col1, col2 = st.columns([1, 3])
    
    with col1:
        # Table selection sidebar
        st.write("Available Tables:")
        
        # Refresh button to update table list
        if st.button("Refresh Tables"):
            with st.spinner("Refreshing table list..."):
                table_names = list_lancedb_tables(st.session_state.lancedb_connection)
                st.session_state.lancedb_tables = table_names
                st.rerun()
        
        # Display table list
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
            st.info("No tables available. Create a table to get started.")
            
            # Option to create a new table
            with st.expander("Create New Table"):
                create_new_table()
    
    with col2:
        # Table viewer panel
        if st.session_state.current_table:
            display_table_details(st.session_state.current_table)


def create_new_table():
    """Interface for creating a new LanceDB table."""
    new_table_name = st.text_input("New Table Name", placeholder="my_table")
    
    # Simple sample data creation
    st.write("Add some sample data:")
    
    # Simple column definition
    cols_text = st.text_area(
        "Column names (one per line)",
        placeholder="id\nname\nvalue\nembedding"
    )
    
    # Sample data creation
    if cols_text:
        columns = [col.strip() for col in cols_text.split("\n") if col.strip()]
        
        if st.button("Create Table"):
            if not new_table_name:
                st.error("Please provide a table name")
            elif not columns:
                st.error("Please define at least one column")
            else:
                try:
                    with st.spinner(f"Creating table '{new_table_name}'..."):
                        # Create an empty DataFrame with the specified columns
                        sample_data = pd.DataFrame({col: [] for col in columns})
                        
                        # Create the table
                        db = st.session_state.lancedb_connection
                        db.create_table(new_table_name, sample_data)
                        
                        # Update table list
                        table_names = list_lancedb_tables(db)
                        st.session_state.lancedb_tables = table_names
                        st.session_state.current_table = new_table_name
                        
                        st.success(f"Created table '{new_table_name}'")
                        st.rerun()
                except Exception as e:
                    st.error(f"Error creating table: {str(e)}")


def display_table_details(table_name):
    """
    Display details and data for a selected LanceDB table.
    
    Args:
        table_name: Name of the table to display
    """
    try:
        # Get table from connection
        db = st.session_state.lancedb_connection
        table = db.open_table(table_name)
        
        # Display table information
        st.subheader(f"Table: {table_name}")
        
        # Table operations
        tab1, tab2, tab3 = st.tabs(["Preview Data", "Table Schema", "Execute Query"])
        
        # Preview tab
        with tab1:
            # Limit control and action buttons
            col1, col2 = st.columns([3, 1])
            with col1:
                limit = st.slider("Number of rows to preview", 5, 100, 10)
            with col2:
                st.write("Actions:")
                if st.button("Delete Selected", key="delete_btn"):
                    if "selected_rows" in st.session_state and st.session_state.selected_rows:
                        with st.spinner("Deleting selected rows..."):
                            try:
                                # Get indices of selected rows
                                indices = sorted(list(st.session_state.selected_rows.keys()), key=int)
                                
                                # Delete rows
                                if table.delete_rows(indices):
                                    st.success(f"Successfully deleted {len(indices)} row(s)")
                                    
                                    # Clear selection
                                    st.session_state.selected_rows = {}
                                    
                                    # Rerun to update UI
                                    st.rerun()
                                else:
                                    st.error("Failed to delete rows")
                            except Exception as e:
                                st.error(f"Error deleting rows: {str(e)}")
                    else:
                        st.warning("No rows selected. Please select rows to delete.")
            
            # Get data preview
            with st.spinner("Loading data preview..."):
                preview_df = table.to_pandas(limit=limit)
                
                # Initialize selected_rows if not present
                if "selected_rows" not in st.session_state:
                    st.session_state.selected_rows = {}
                
                # Display preview with selection
                st.write(f"Showing {len(preview_df)} of {table.count_rows()} rows")
                st.write("Select rows to delete them:")
                
                # Create editable dataframe with selection column
                edited_df = preview_df.copy()
                
                # Display the dataframe with row selection
                selection = st.data_editor(
                    edited_df,
                    use_container_width=True,
                    disabled=edited_df.columns.tolist(),  # Disable editing of all columns
                    hide_index=False,
                    column_config={
                        "_index": st.column_config.Column(
                            "Select",
                            help="Select rows to delete",
                            required=True,
                            width="small",
                        )
                    },
                    key="table_data_editor"
                )
                
                # Store selection in session state
                st.session_state.selected_rows = selection.get("edited_rows", {})
        
        # Schema tab
        with tab2:
            with st.spinner("Loading schema information..."):
                # Get schema information
                schema = table.schema
                
                # Display schema as a table
                schema_data = []
                for field in schema:
                    schema_data.append({
                        "Name": field.name,
                        "Type": str(field.type),
                        "Nullable": field.nullable
                    })
                
                st.write("Table Schema:")
                st.dataframe(pd.DataFrame(schema_data), use_container_width=True)
        
        # Query tab
        with tab3:
            st.write("Execute Query on Table")
            
            # Simple query interface
            query_type = st.selectbox(
                "Query Type",
                ["Basic Filter", "Vector Search"]
            )
            
            if query_type == "Basic Filter":
                # Only show if we have schema information
                if schema_data:
                    # Get columns that are not vectors/embeddings
                    non_vector_cols = [field["Name"] for field in schema_data 
                                      if "vector" not in field["Type"].lower() and 
                                         "array" not in field["Type"].lower()]
                    
                    if non_vector_cols:
                        # Column selection for filter
                        filter_col = st.selectbox("Filter Column", non_vector_cols)
                        
                        # Simple filter condition
                        filter_op = st.selectbox(
                            "Filter Operator",
                            ["=", ">", "<", ">=", "<=", "!=", "LIKE"]
                        )
                        
                        filter_value = st.text_input("Filter Value")
                        
                        if st.button("Execute Filter Query"):
                            if not filter_value:
                                st.warning("Please enter a filter value")
                            else:
                                with st.spinner("Executing query..."):
                                    try:
                                        # Create query string based on operator
                                        if filter_op == "LIKE":
                                            query_df = table.search(f"{filter_col} LIKE '{filter_value}'").to_pandas()
                                        else:
                                            # Try to convert to numeric if possible
                                            try:
                                                numeric_value = float(filter_value)
                                                query_df = table.search(f"{filter_col} {filter_op} {numeric_value}").to_pandas()
                                            except ValueError:
                                                # Use as string with quotes
                                                query_df = table.search(f"{filter_col} {filter_op} '{filter_value}'").to_pandas()
                                        
                                        # Display results
                                        st.write(f"Query Results: {len(query_df)} rows")
                                        st.dataframe(query_df, use_container_width=True)
                                        
                                        # Option to load into session state as the current dataset
                                        if st.button("Load as Current Dataset"):
                                            st.session_state.data = query_df
                                            st.success("Data loaded into current dataset!")
                                    except Exception as e:
                                        st.error(f"Error executing query: {str(e)}")
                    else:
                        st.info("No suitable columns found for filtering")
                else:
                    st.info("Schema information not available")
            else:
                # Vector search interface
                st.write("Vector similarity search is not implemented in this basic version.")
                st.info("To implement vector search, you would need to collect vector inputs and use LanceDB's vector search capabilities.")
    
    except Exception as e:
        st.error(f"Error displaying table details: {str(e)}")