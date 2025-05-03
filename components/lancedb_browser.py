import streamlit as st
import pandas as pd
import os
import json
from pathlib import Path

import lancedb

#  connection function to replace lancedb.connect
def connect(uri):
    """Mock connect function that returns a MockLanceDB instance."""
    db = lancedb.connect(uri)
    return db

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
    with st.expander("LanceDB Connection", expanded=not st.session_state.lancedb_connected):
        connect_to_lancedb()
    
    # If connected, show table browser
    if st.session_state.lancedb_connected and st.session_state.lancedb_connection:
        display_table_browser()


def connect_to_lancedb():
    """Handle connection to LanceDB database."""
    st.subheader("Connect to LanceDB")
    # Local directory connection
    db_path = st.text_input(
        "Database Path",
        value=".lancedb",
        help="Path to the local LanceDB database directory"
    )
    
    if st.button("Connect to Local Database"):
        try:
            with st.spinner("Connecting to LanceDB..."):
                # Create directory if it doesn't exist
                #os.makedirs(db_path, exist_ok=True)
                
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

def list_lancedb_tables(db):
    """
    Get list of tables from a LanceDB connection.
    
    Args:
        db: LanceDB connection object
        
    Returns:
        List of table names
    """
    try:
        tables = db.table_names()
        st.session_state.lancedb_tables = tables
        print(tables)
        return tables
    except Exception as e:
        st.error(f"Error listing tables: {str(e)}")
        return []


def display_table_browser():
    db = st.session_state.lancedb_connection
    """Display the browser interface for LanceDB tables."""
    st.subheader("Browse Tables")
    
    col1, col2 = st.columns([1, 3])
    
    with col1:
        # Table selection sidebar
        st.write("Available Tables:")
        
        # Refresh button to update table list
        if st.button("Refresh Tables"):
            with st.spinner("Refreshing table list..."):
                table_names = list_lancedb_tables(db)
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
            
            # # Option to create a new table
            # with st.expander("Create New Table"):
            #     create_new_table()
    
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
        try:
            table = db.open_table(table_name)
        except Exception as e:
            st.error(f"Error opening table: {str(e)}")
            return
        
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
                # Delete button will be moved after loading preview_df
            
            # Get data preview
            with st.spinner("Loading data preview..."):
                # Use the limit from the slider to limit the number of rows
                preview_df = table.to_pandas().head(limit)
                
                # Initialize selected_rows if not present
                if "selected_rows" not in st.session_state:
                    st.session_state.selected_rows = {}
                
                # Display preview with selection
                st.write(f"Showing {len(preview_df)} of {table.count_rows()} rows")
                st.write("Select rows to delete them:")
                
                # Create editable dataframe with selection column
                edited_df = preview_df.copy()
                
                # Display the dataframe with row selection
                # Add a selection column to the dataframe
                # Create a checkbox column for selection
                edited_df = edited_df.copy()
                edited_df.insert(0, "Select", False)
                
                selection = st.data_editor(
                    edited_df,
                    use_container_width=True,
                    disabled=edited_df.columns.tolist()[1:],  # Disable editing of all columns except Select
                    hide_index=False,
                    key="table_data_editor"
                )
                
                # Store selection in session state
                # Get the indices of rows where Select is True
                selected_indices = {}
                for i, row in enumerate(selection["Select"]):
                    if row:
                        selected_indices[str(i)] = True
                
                st.session_state.selected_rows = selected_indices
                
                # Now add the delete button after preview_df is defined
                if st.button("Delete Selected", key="delete_btn"):
                    if "selected_rows" in st.session_state and st.session_state.selected_rows:
                        with st.spinner("Deleting selected rows..."):
                            try:
                                # Get indices of selected rows
                                indices = sorted(list(st.session_state.selected_rows.keys()), key=int)
                                
                                # Delete rows using the proper LanceDB API
                                # LanceDB doesn't have a delete_rows method, but it has a delete method
                                # that takes a filter condition
                                
                                # Get the id values of the selected rows
                                # The indices are now the row numbers in the preview dataframe
                                id_values = [preview_df.iloc[int(idx)]['id'] for idx in indices if int(idx) < len(preview_df)]
                                
                                if id_values:
                                    # Create a filter condition for the ids
                                    # Use proper SQL syntax for the WHERE clause
                                    if len(id_values) == 1:
                                        # For a single value, use equality
                                        filter_condition = f"id = {id_values[0]}"
                                    else:
                                        # For multiple values, use IN with parentheses
                                        filter_condition = f"id IN ({','.join(map(str, id_values))})"
                                    
                                    # Delete the rows
                                    table.delete(filter_condition)
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