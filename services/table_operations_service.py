"""
Table Operations Service

This module provides table operation functionality using the LanceDB service.
"""
from typing import List, Dict, Any, Optional, Union, IO
import pandas as pd
from services.lancedb_service import LanceDBService
from services.embedding_service import EmbeddingService
from utils.error_utils import with_error_handling, ValidationError, validate_table_name
import logging

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class TableOperationsService:
    """
    Service for handling table operations.
    Uses LanceDBService for all database interactions.
    """
    
    def __init__(self, db_service: LanceDBService, embedding_service: Optional[EmbeddingService] = None):
        self.db_service = db_service
        self.embedding_service = embedding_service
    
    @with_error_handling()
    def list_tables(self) -> List[str]:
        """
        Get list of all tables.
        
        Returns:
            List of table names
        """
        return self.db_service.list_tables()
    
    @with_error_handling()
    def get_table_data(self, table_name: str, limit: int = 100) -> Dict[str, Any]:
        """
        Get data from a table.
        
        Args:
            table_name: Table to query
            limit: Maximum number of rows
            
        Returns:
            Dictionary with table data and metadata
        """
        validate_table_name(table_name)
        
        # Get the data
        df = self.db_service.query_table(table_name, limit)
        
        # Get schema information
        table = self.db_service.get_connection()[table_name]
        schema = {
            field.name: str(field.type)
            for field in table.schema
        }
        
        return {
            'data': df,
            'schema': schema,
            'row_count': len(df),
            'total_columns': len(df.columns)
        }
    
    @with_error_handling()
    def create_table(self, table_name: str, data: Union[pd.DataFrame, IO],
                    vector_column: Optional[str] = None) -> Dict[str, Any]:
        """
        Create a new table.
        
        Args:
            table_name: Name for the new table
            data: DataFrame or file-like object (CSV/Parquet)
            vector_column: Optional name of vector/embedding column
            
        Returns:
            Dictionary with creation result
        """
        validate_table_name(table_name)
        
        # Convert file-like object to DataFrame if needed
        if not isinstance(data, pd.DataFrame):
            df = self._load_data_from_file(data)
        else:
            df = data
            
        # Create the table
        self.db_service.create_table(table_name, df, vector_column)
        
        return {
            'table_name': table_name,
            'row_count': len(df),
            'column_count': len(df.columns),
            'schema': df.dtypes.to_dict()
        }
    
    def _load_data_from_file(self, file_obj: IO) -> pd.DataFrame:
        """
        Load data from a file object into a DataFrame.
        
        Args:
            file_obj: File-like object
            
        Returns:
            Loaded DataFrame
            
        Raises:
            ValidationError: If file format is not supported
        """
        if hasattr(file_obj, 'name'):
            if file_obj.name.endswith('.csv'):
                return pd.read_csv(file_obj)
            elif file_obj.name.endswith('.parquet'):
                return pd.read_parquet(file_obj)
            else:
                raise ValidationError(
                    "Unsupported file type",
                    {'supported_formats': ['.csv', '.parquet']}
                )
        
        # Try CSV as default
        try:
            return pd.read_csv(file_obj)
        except Exception as e:
            raise ValidationError(
                "Could not parse file",
                {'error': str(e)}
            )
    
    @with_error_handling()
    def get_table_schema(self, table_name: str) -> Dict[str, Dict[str, Any]]:
        """
        Get schema information for a table.
        
        Args:
            table_name: Table to get schema for
            
        Returns:
            Dictionary with schema information
        """
        validate_table_name(table_name)
        
        table = self.db_service.get_connection()[table_name]
        
        schema_info = {}
        for field in table.schema:
            schema_info[field.name] = {
                'type': str(field.type),
                'nullable': field.nullable,
                'is_vector': 'list' in str(field.type).lower() or 'array' in str(field.type).lower()
            }
        
        return schema_info
    
    @with_error_handling()
    def get_non_vector_columns(self, table_name: str) -> List[str]:
        """
        Get list of non-vector columns in a table.
        
        Args:
            table_name: Table to analyze
            
        Returns:
            List of column names
        """
        schema_result = self.get_table_schema(table_name)
        if not isinstance(schema_result, dict) or 'data' not in schema_result:
            logger.error(f"Unexpected schema result: {schema_result}")
            return []
            
        schema = schema_result['data']
        return [
            name for name, info in schema.items()
            if not info.get('is_vector', False)
        ]
    
    @with_error_handling()
    def create_sample_table(self, table_name: str, columns: List[str],
                          sample_size: int = 5) -> Dict[str, Any]:
        """
        Create a table with sample data.
        
        Args:
            table_name: Name for the new table
            columns: List of column names
            sample_size: Number of sample rows
            
        Returns:
            Dictionary with creation result
        """
        validate_table_name(table_name)
        
        # Generate sample data
        sample_data = {}
        for col in columns:
            if col.lower() == 'id':
                sample_data[col] = list(range(1, sample_size + 1))
            elif any(kw in col.lower() for kw in ['embedding', 'vector']):
                sample_data[col] = [[0.1, 0.2, 0.3]] * sample_size
            elif any(kw in col.lower() for kw in ['int', 'num', 'count']):
                sample_data[col] = [i * 10 for i in range(1, sample_size + 1)]
            elif any(kw in col.lower() for kw in ['float', 'decimal', 'price']):
                sample_data[col] = [i * 10.5 for i in range(1, sample_size + 1)]
            else:
                sample_data[col] = [f"Sample {col} {i}" for i in range(1, sample_size + 1)]
        
        df = pd.DataFrame(sample_data)
        
        # Create the table
        return self.create_table(table_name, df)
    
    @with_error_handling()
    def create_embeddings(self, table_name: str, selected_fields: List[str], 
                         embedding_column: str = 'embedding',
                         model_name: str = 'all-MiniLM-L6-v2') -> Dict[str, Any]:
        """
        Create embeddings from selected fields and add them to the table.
        
        Args:
            table_name: Name of the table
            selected_fields: List of fields to combine for embedding
            embedding_column: Name of the column to store embeddings
            model_name: Name of the model to use for embeddings
            
        Returns:
            Dictionary with operation results
            
        Raises:
            ValidationError: If validation fails
            ValueError: If embedding service is not available
        """
        if not self.embedding_service:
            raise ValueError("Embedding service not initialized")
            
        validate_table_name(table_name)
        
        # Get the table data
        table = self.db_service.get_connection()[table_name]
        df = pd.DataFrame(table.to_pandas())
        
        if not all(field in df.columns for field in selected_fields):
            missing = [f for f in selected_fields if f not in df.columns]
            raise ValidationError(
                "Some selected fields not found in table",
                {'missing_fields': missing}
            )
        
        # Combine selected fields into text for embedding
        texts = []
        for _, row in df.iterrows():
            combined_text = " ".join(str(row[field]) for field in selected_fields)
            texts.append(combined_text)
        
        # Generate embeddings
        embeddings = self.embedding_service.generate_batch_embeddings(texts, model_name)
        
        # Add embeddings to the table
        df[embedding_column] = embeddings
        
        # Delete the existing table first
        self.db_service.delete_table(table_name)
        
        # Create new table with embeddings
        self.db_service.create_table(
            table_name=table_name,
            data=df,
            vector_column=embedding_column
        )
        
        return {
            'table_name': table_name,
            'embedding_column': embedding_column,
            'num_embeddings': len(embeddings),
            'embedding_dimension': len(embeddings[0]) if embeddings else 0,
            'source_fields': selected_fields
        }

    @with_error_handling()
    def delete_rows(self, table_name: str, filter_condition: Dict[str, Any]) -> Dict[str, Any]:
        """
        Delete rows from a table based on a filter condition.
        
        Args:
            table_name: Name of the table
            filter_condition: Dictionary of column-value pairs to filter by
            
        Returns:
            Dictionary with operation results
            
        Raises:
            ValidationError: If validation fails
        """
        validate_table_name(table_name)
        
        # Get the table data
        table = self.db_service.get_connection()[table_name]
        df = pd.DataFrame(table.to_pandas())
        initial_row_count = len(df)
        
        # Get schema to identify vector columns
        schema = self.get_table_schema(table_name)
        if not isinstance(schema, dict) or 'data' not in schema:
            raise ValidationError("Failed to get table schema")
            
        vector_columns = [
            name for name, info in schema['data'].items()
            if info.get('is_vector', False)
        ]
        
        # Remove vector columns from filter condition
        for col in vector_columns:
            filter_condition.pop(col, None)
        
        # Create filter mask
        mask = pd.Series(True, index=df.index)
        for column, value in filter_condition.items():
            if column not in df.columns:
                raise ValidationError(
                    f"Column '{column}' not found in table",
                    {'available_columns': list(df.columns)}
                )
            
            # Convert value to match column type
            try:
                col_type = df[column].dtype
                if pd.api.types.is_numeric_dtype(col_type):
                    value = float(value) if isinstance(value, (int, float)) else value
                elif pd.api.types.is_bool_dtype(col_type):
                    value = bool(value)
                elif pd.api.types.is_datetime64_dtype(col_type):
                    value = pd.to_datetime(value)
                
                # Compare values
                if pd.api.types.is_numeric_dtype(col_type):
                    mask &= (df[column].astype(float) == float(value))
                else:
                    mask &= (df[column].astype(str) == str(value))
                
            except Exception as e:
                raise ValidationError(f"Error comparing column {column}: {str(e)}")
        
        # Count rows to be deleted
        rows_to_delete = mask.sum()
        
        if rows_to_delete == 0:
            return {
                'table_name': table_name,
                'rows_deleted': 0,
                'remaining_rows': initial_row_count,
                'warning': 'No rows matched the filter condition'
            }
        
        # Filter out rows to keep
        df = df[~mask]
        
        # Verify we actually removed rows
        if len(df) >= initial_row_count:
            return {
                'table_name': table_name,
                'rows_deleted': 0,
                'remaining_rows': initial_row_count,
                'error': 'No rows were removed after filtering'
            }
        
        # Replace the table with filtered data
        try:
            self.db_service.replace_table(table_name, df)
        except Exception as e:
            raise
        
        return {
            'table_name': table_name,
            'rows_deleted': initial_row_count - len(df),
            'remaining_rows': len(df)
        }

    @with_error_handling()
    def get_table_data_paginated(self, table_name: str, page: int = 1, 
                               page_size: int = 50) -> Dict[str, Any]:
        """
        Get paginated table data.
        
        Args:
            table_name: Table to query
            page: Page number (1-based)
            page_size: Number of rows per page
            
        Returns:
            Dictionary with paginated data and metadata
        """
        validate_table_name(table_name)
        
        offset = (page - 1) * page_size
        df = self.db_service.query_table_paginated(table_name, offset, page_size)
        
        # Get total rows and handle the wrapped response
        total_rows_result = self.get_table_row_count(table_name)
        if isinstance(total_rows_result, dict) and 'data' in total_rows_result:
            total_rows = total_rows_result['data']
        else:
            total_rows = total_rows_result
        
        return {
            'data': df,
            'current_page': page,
            'page_size': page_size,
            'total_rows': total_rows,
            'total_pages': (total_rows + page_size - 1) // page_size,
            'has_next': page * page_size < total_rows,
            'has_previous': page > 1
        }

    @with_error_handling()
    def get_table_row_count(self, table_name: str) -> int:
        """
        Get total row count for a table.
        
        Args:
            table_name: Table to count
            
        Returns:
            Total number of rows
        """
        validate_table_name(table_name)
        return self.db_service.count_table_rows(table_name) 