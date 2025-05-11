"""
LanceDB Service Module

This module provides core functionality for interacting with LanceDB,
completely decoupled from any UI framework.
"""
import os
import pandas as pd
import logging
import traceback
from typing import List, Dict, Any, Optional, Tuple, Union

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Set to DEBUG level for more verbose output

# Add a console handler if not already present
if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)


class LanceDBService:
    """
    Service class for LanceDB operations.
    This class encapsulates all operations related to LanceDB and is completely
    independent of any UI framework.
    """
    
    def __init__(self):
        logger.debug("Initializing LanceDBService")
        self.connection = None
        self.db_path = None
    
    def connect(self, db_path: str) -> Any:
        """
        Connect to a LanceDB database.
        
        Args:
            db_path: Path to the LanceDB database
            
        Returns:
            LanceDB connection object
            
        Raises:
            Exception: If connection fails
        """
        import lancedb
        try:
            # Store the path for potential reconnection
            self.db_path = db_path
            
            logger.debug(f"Connecting to LanceDB at: {db_path}")
            logger.debug(f"Path exists check: {os.path.exists(db_path)}")
            
            db = lancedb.connect(db_path)
            self.connection = db
            logger.debug("Connection successful")
            return db
        except Exception as e:
            # Reset connection state on failure
            self.connection = None
            logger.error(f"Failed to connect to LanceDB: {str(e)}")
            logger.debug(traceback.format_exc())
            raise Exception(f"Failed to connect to LanceDB: {str(e)}")
            
    def ensure_connection(self) -> bool:
        """
        Ensure that the connection to the database is active.
        If not connected but we have a db_path, try to reconnect.
        
        Returns:
            True if connection is active, False otherwise
        """
        logger.debug(f"Checking connection status: connection={self.connection is not None}, db_path={self.db_path}")
        
        if self.connection is not None:
            try:
                # Test the connection by listing tables
                _ = self.connection.table_names()
                logger.debug("Connection is valid")
                return True
            except Exception as e:
                logger.debug(f"Connection test failed: {str(e)}")
                self.connection = None
            
        if self.db_path:
            try:
                logger.debug(f"Attempting to reconnect to {self.db_path}")
                self.connect(self.db_path)
                return True
            except Exception as e:
                logger.error(f"Failed to reconnect to LanceDB: {str(e)}")
                return False
        
        logger.debug("No connection and no db_path")
        return False
    
    def list_tables(self) -> List[str]:
        """
        List all tables in the connected LanceDB.
        
        Returns:
            List of table names
            
        Raises:
            Exception: If not connected to a database
        """
        logger.debug("Listing tables")
        if not self.ensure_connection():
            logger.error("Cannot list tables - not connected")
            raise Exception("Not connected to a LanceDB database")
        
        try:
            tables = self.connection.table_names()
            logger.debug(f"Found tables: {tables}")
            return tables
        except Exception as e:
            logger.error(f"Failed to list tables: {str(e)}")
            logger.debug(traceback.format_exc())
            # Only reset connection for connection-related errors
            if "connection" in str(e).lower():
                self.connection = None
            raise Exception(f"Failed to list tables: {str(e)}")
    
    def get_table(self, table_name: str) -> Any:
        """
        Get a reference to a specific table.
        
        Args:
            table_name: Name of the table to retrieve
            
        Returns:
            LanceDB table object
            
        Raises:
            Exception: If table doesn't exist or not connected
        """
        logger.debug(f"Getting table: {table_name}")
        if not self.ensure_connection():
            logger.error("Cannot get table - not connected")
            raise Exception("Not connected to a LanceDB database")
            
        try:
            table = self.connection[table_name]
            return table
        except Exception as e:
            logger.error(f"Failed to get table '{table_name}': {str(e)}")
            logger.debug(traceback.format_exc())
            # Only reset connection for connection-related errors
            if "connection" in str(e).lower():
                self.connection = None
            raise Exception(f"Failed to get table '{table_name}': {str(e)}")
    
    def query_table(self, table_name: str, limit: int = 100) -> pd.DataFrame:
        """
        Query data from a table.
        
        Args:
            table_name: Table to query
            limit: Maximum number of rows to return
            
        Returns:
            DataFrame with query results
            
        Raises:
            Exception: If query fails
        """
        logger.debug(f"Querying table: {table_name} with limit: {limit}")
        if not self.ensure_connection():
            logger.error("Cannot query table - not connected")
            raise Exception("Not connected to a LanceDB database")
            
        try:
            table = self.get_table(table_name)
            logger.debug(f"Table obtained successfully, converting to pandas")
            result = table.search().limit(limit).to_pandas()
            logger.debug(f"Query successful, got {len(result)} rows")
            return result
        except Exception as e:
            logger.error(f"Failed to query table '{table_name}': {str(e)}")
            logger.debug(traceback.format_exc())
            # Only reset connection for connection-related errors
            if "connection" in str(e).lower():
                self.connection = None
            raise Exception(f"Failed to query table '{table_name}': {str(e)}")
    
    def create_table(self, table_name: str, data: pd.DataFrame, 
                    vector_column: Optional[str] = None) -> Any:
        """
        Create a new table in LanceDB.
        
        Args:
            table_name: Name for the new table
            data: DataFrame with the data to store
            vector_column: Optional name of vector column
            
        Returns:
            Newly created table
            
        Raises:
            Exception: If table creation fails
        """
        logger.debug(f"Creating table: {table_name}")
        if not self.ensure_connection():
            logger.error("Cannot create table - not connected")
            raise Exception("Not connected to a LanceDB database")
            
        try:
            # Check if the table already exists
            existing_tables = self.list_tables()
            if table_name in existing_tables:
                raise Exception(f"Table '{table_name}' already exists")
                
            # Create the table with data
            table = self.connection.create_table(table_name, data=data)
            logger.debug(f"Table created successfully")
            return table
        except Exception as e:
            logger.error(f"Failed to create table '{table_name}': {str(e)}")
            logger.debug(traceback.format_exc())
            # Only reset connection for connection-related errors
            if "connection" in str(e).lower():
                self.connection = None
            raise Exception(f"Failed to create table '{table_name}': {str(e)}")
    
    def get_connection(self) -> Any:
        """
        Get the raw database connection object.
        
        Returns:
            The database connection object if connected, otherwise None.
        """
        logger.debug("Getting database connection")
        if not self.ensure_connection():
            logger.debug("No active connection available")
            return None
        
        return self.connection
    
    def semantic_search(self, table_name: str, query_vector: List[float], 
                      limit: int = 10) -> pd.DataFrame:
        """
        Perform semantic search in a vector table.
        
        Args:
            table_name: Table to search in
            query_vector: Vector to search for
            limit: Maximum number of results
            
        Returns:
            DataFrame with search results
            
        Raises:
            Exception: If search fails
        """
        logger.debug(f"Performing semantic search in: {table_name}")
        if not self.ensure_connection():
            logger.error("Cannot perform search - not connected")
            raise Exception("Not connected to a LanceDB database")
            
        try:
            table = self.get_table(table_name)
            result = table.search(query_vector).limit(limit).to_pandas()
            result.sort_values(by=['_distance'], inplace=True, ascending=True)
            logger.debug(f"Search successful, got {len(result)} results")
            return result
        except Exception as e:
            logger.error(f"Failed to perform semantic search in '{table_name}': {str(e)}")
            logger.debug(traceback.format_exc())
            # Only reset connection for connection-related errors
            if "connection" in str(e).lower():
                self.connection = None
            raise Exception(f"Failed to perform semantic search in '{table_name}': {str(e)}")
