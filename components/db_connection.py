import streamlit as st
import os
import lancedb

def is_running_in_docker():
    """Check if the application is running inside a Docker container."""
    try:
        with open('/proc/self/cgroup', 'r') as f:
            return any('docker' in line for line in f)
    except:
        # Fallback to environment variable check
        return os.environ.get('RUNNING_IN_DOCKER', 'false').lower() == 'true'

def connect(uri):
    """Connect function that returns a LanceDB instance."""
    db = lancedb.connect(uri)
    return db

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