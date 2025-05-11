"""
LanceDB Browser Component

This is the main entry point for the LanceDB Browser application.
It initializes and runs the StreamlitAdapter.
"""
import streamlit as st
from adapters.streamlit_adapter import StreamlitAdapter

def lancedb_browser():
    """
    Initialize and run the LanceDB Browser application.
    Uses the StreamlitAdapter to handle all UI and database interactions.
    """
    # Create the adapter instance
    adapter = StreamlitAdapter()
    
    # Run the main browser interface
    adapter.run_browser_interface()

if __name__ == "__main__":
    lancedb_browser()