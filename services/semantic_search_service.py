"""
Semantic Search Service

This module provides semantic search functionality using the LanceDB and Embedding services.
"""
from typing import List, Dict, Any, Tuple, Optional
import pandas as pd
from services.embedding_service import EmbeddingService
from services.lancedb_service import LanceDBService
from utils.error_utils import with_error_handling, ValidationError, validate_vector_dimension
import logging

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class SemanticSearchService:
    """
    Service for handling semantic search operations.
    Uses EmbeddingService for vector generation and LanceDBService for search.
    """
    
    def __init__(self, db_service: LanceDBService, embedding_service: EmbeddingService):
        self.db_service = db_service
        self.embedding_service = embedding_service
    
    @with_error_handling()
    def get_embedding_tables(self) -> List[Tuple[str, str]]:
        """
        Get list of tables with embedding columns.
        
        Returns:
            List of tuples (table_name, embedding_column)
        """
        tables_with_embeddings = []
        for table_name in self.db_service.list_tables():
            table = self.db_service.get_connection()[table_name]
            for field in table.schema:
                # Check for likely embedding fields
                if any(kw in field.name.lower() for kw in ["embedding", "vector"]) or \
                   "list" in str(field.type).lower() or \
                   "array" in str(field.type).lower():
                    tables_with_embeddings.append((table_name, field.name))
        return tables_with_embeddings
    
    @with_error_handling()
    def get_available_models(self) -> Dict[str, Dict[str, Any]]:
        """
        Get available embedding models.
        
        Returns:
            Dictionary of model information
        """
        return self.embedding_service.get_available_models()
    
    @with_error_handling()
    def search_by_text(self, table_name: str, query_text: str, 
                      embedding_column: str, model_name: str = 'all-MiniLM-L6-v2',
                      limit: int = 10) -> pd.DataFrame:
        """
        Perform semantic search using text query.
        
        Args:
            table_name: Table to search in
            query_text: Text to search for
            embedding_column: Column containing embeddings
            model_name: Model to use for embedding generation
            limit: Maximum number of results
            
        Returns:
            DataFrame with search results
        """
        # Generate embedding for query
        query_vector = self.embedding_service.generate_embedding(query_text, model_name)
        
        # Get expected dimension
        expected_dim = self.embedding_service.get_embedding_dimension(model_name)
        validate_vector_dimension(query_vector, expected_dim)
        
        # Perform search
        return self.db_service.semantic_search(
            table_name=table_name,
            query_vector=query_vector,
            vector_column=embedding_column,
            limit=limit
        )
    
    @with_error_handling()
    def search_by_vector(self, table_name: str, query_vector: List[float],
                        embedding_column: str, expected_dim: Optional[int] = None,
                        limit: int = 10) -> pd.DataFrame:
        """
        Perform search with pre-computed vector.
        
        Args:
            table_name: Table to search in
            query_vector: Vector to search with
            embedding_column: Column containing embeddings
            expected_dim: Expected vector dimension (optional)
            limit: Maximum number of results
            
        Returns:
            DataFrame with search results
        """
        # Validate vector dimension if provided
        if expected_dim is not None:
            validate_vector_dimension(query_vector, expected_dim)
            
        return self.db_service.semantic_search(
            table_name=table_name,
            query_vector=query_vector,
            vector_column=embedding_column,
            limit=limit
        )
    
    @with_error_handling()
    def process_search_results(self, results: pd.DataFrame,
                             exclude_columns: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Process and format search results.
        
        Args:
            results: Search results DataFrame
            exclude_columns: Columns to exclude from output
            
        Returns:
            Dictionary with processed results
        """
        if exclude_columns is None:
            exclude_columns = []
            
        # Add default columns to exclude
        #exclude_columns.extend(['_distance'])
        
        # Filter columns
        display_columns = [col for col in results.columns if col not in exclude_columns]
        
        return {
            'total_results': len(results),
            'columns': display_columns,
            'results': results[display_columns].to_dict(orient='records'),
            'distances': results['_distance'].tolist() if '_distance' in results else None
        } 