import streamlit as st
import pandas as pd

def display_create_table_tab(db, list_lancedb_tables):
    """Interface for creating a new LanceDB table in its own tab."""
    st.subheader("Create New Table")
    create_new_table(db, list_lancedb_tables)

def create_new_table(db, list_lancedb_tables):
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
                        # Create a DataFrame with sample data for the specified columns
                        # Generate 5 rows of sample data
                        sample_data = {}
                        for col in columns:
                            if col.lower() == 'id':
                                # For id column, use sequential integers
                                sample_data[col] = list(range(1, 6))
                            elif 'embedding' in col.lower():
                                # For embedding columns, use simple vectors
                                sample_data[col] = [[0.1, 0.2, 0.3]] * 5
                            elif any(type_hint in col.lower() for type_hint in ['int', 'num', 'count']):
                                # For numeric columns, use random integers
                                sample_data[col] = [i * 10 for i in range(1, 6)]
                            elif any(type_hint in col.lower() for type_hint in ['float', 'decimal', 'price']):
                                # For float columns, use decimals
                                sample_data[col] = [i * 10.5 for i in range(1, 6)]
                            else:
                                # For other columns, use text
                                sample_data[col] = [f"Sample {col} {i}" for i in range(1, 6)]
                        
                        sample_data = pd.DataFrame(sample_data)
                        
                        # Create the table
                        db.create_table(new_table_name, sample_data)
                        
                        # Update table list
                        table_names = list_lancedb_tables(db)
                        st.session_state.lancedb_tables = table_names
                        st.session_state.current_table = new_table_name
                        
                        st.success(f"Created table '{new_table_name}'")
                        st.rerun()
                except Exception as e:
                    st.error(f"Error creating table: {str(e)}")