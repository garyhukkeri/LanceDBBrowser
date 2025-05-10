"""
LanceDB Service Module

This module provides core functionality for interacting with LanceDB,
completely decoupled from any UI framework.
"""
import os
import pandas as pd
import logging
from typing import List, Dict, Any, Optional, Tuple, Union

# Set up logging
logger = logging.getLogger(__name__)


class LanceDBService:
    """
    Service class for LanceDB operations.
    This class encapsulates all operations related to LanceDB and is completely
    independent of any UI framework.
    """
    
    def __init__(self):
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
            db = lancedb.connect(db_path)
            self.connection = db
            return db
        except Exception as e:
            # Reset connection state on failure
            self.connection = None
            logger.error(f"Failed to connect to LanceDB: {str(e)}")
            raise Exception(f"Failed to connect to LanceDB: {str(e)}")
            
    def ensure_connection(self) -> bool:
        """
        Ensure that the connection to the database is active.
        If not connected but we have a db_path, try to reconnect.
        
        Returns:
            True if connection is active, False otherwise
        """
        if self.connection is not None:
            return True
            
        if self.db_path:
            try:
                self.connect(self.db_path)
                return True
            except Exception as e:
                logger.error(f"Failed to reconnect to LanceDB: {str(e)}")
                return False
        
        return False
    
    def list_tables(self) -> List[str]:
        """
        List all tables in the connected LanceDB.
        
        Returns:
            List of table names
            
        Raises:
            Exception: If not connected to a database
        """
        if not self.ensure_connection():
            raise Exception("Not connected to a LanceDB database")
        
        try:
            tables = self.connection.table_names()
            return tables
        except Exception as e:
            logger.error(f"Failed to list tables: {str(e)}")
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
        if not self.ensure_connection():
            raise Exception("Not connected to a LanceDB database")
            
        try:
            return self.connection[table_name]
        except Exception as e:
            logger.error(f"Failed to get table '{table_name}': {str(e)}")
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
        if not self.ensure_connection():
            raise Exception("Not connected to a LanceDB database")
            
        try:
            table = self.get_table(table_name)
            result = table.to_pandas(limit=limit)
            return result
        except Exception as e:
            logger.error(f"Failed to query table '{table_name}': {str(e)}")
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
        if not self.ensure_connection():
            raise Exception("Not connected to a LanceDB database")
            
        try:
            # Check if the table already exists
            existing_tables = self.list_tables()
            if table_name in existing_tables:
                raise Exception(f"Table '{table_name}' already exists")
                
            # Create the table with data
            table = self.connection.create_table(table_name, data=data)
            return table
        except Exception as e:
            logger.error(f"Failed to create table '{table_name}': {str(e)}")
            raise Exception(f"Failed to create table '{table_name}': {str(e)}")
    
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
        if not self.ensure_connection():
            raise Exception("Not connected to a LanceDB database")
            
        try:
            table = self.get_table(table_name)
            result = table.search(query_vector).limit(limit).to_pandas()
            result.sort_values(by=['_distance'], inplace=True, ascending=True)
            return result
        except Exception as e:
            logger.error(f"Failed to perform semantic search in '{table_name}': {str(e)}")
            raise Exception(f"Failed to perform semantic search in '{table_name}': {str(e)}")
