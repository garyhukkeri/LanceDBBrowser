"""
Embedding Service Module

This module provides a service for managing embeddings and models.
Includes model caching and embedding generation functionality.
"""
from typing import List, Dict, Any, Optional
import logging
from sentence_transformers import SentenceTransformer
import numpy as np
from functools import lru_cache

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class ModelNotFoundError(Exception):
    """Exception for when a requested model is not available"""
    pass

class EmbeddingError(Exception):
    """Exception for embedding generation errors"""
    pass

class EmbeddingService:
    """
    Service for managing embeddings and models.
    Handles model loading, caching, and embedding generation.
    """
    
    # Default models that are known to work well
    DEFAULT_MODELS = {
        'all-MiniLM-L6-v2': {
            'dimension': 384,
            'description': 'Good general purpose model, fast and efficient'
        },
        'paraphrase-MiniLM-L6-v2': {
            'dimension': 384,
            'description': 'Optimized for paraphrase detection and similarity'
        },
        'all-mpnet-base-v2': {
            'dimension': 768,
            'description': 'Higher quality, but slower and larger'
        }
    }
    
    def __init__(self):
        self._model_cache: Dict[str, SentenceTransformer] = {}
        logger.info("Initializing EmbeddingService")
    
    def get_available_models(self) -> Dict[str, Dict[str, Any]]:
        """
        Get information about available embedding models.
        
        Returns:
            Dictionary of model information
        """
        return self.DEFAULT_MODELS.copy()
    
    def _load_model(self, model_name: str) -> SentenceTransformer:
        """
        Load a model into memory.
        
        Args:
            model_name: Name of the model to load
            
        Returns:
            Loaded model
            
        Raises:
            ModelNotFoundError: If model cannot be loaded
        """
        try:
            logger.info(f"Loading model: {model_name}")
            model = SentenceTransformer(model_name)
            self._model_cache[model_name] = model
            return model
        except Exception as e:
            raise ModelNotFoundError(f"Failed to load model '{model_name}': {str(e)}")
    
    def get_model(self, model_name: str) -> SentenceTransformer:
        """
        Get a model, loading it if necessary.
        
        Args:
            model_name: Name of the model to get
            
        Returns:
            The requested model
            
        Raises:
            ModelNotFoundError: If model is not available
        """
        if model_name not in self._model_cache:
            return self._load_model(model_name)
        return self._model_cache[model_name]
    
    @lru_cache(maxsize=1000)
    def _cached_generate_embedding(self, text: str, model_name: str) -> tuple:
        """
        Generate an embedding with caching.
        Note: Input is converted to tuple for hashability in lru_cache.
        """
        model = self.get_model(model_name)
        vector = model.encode([text], convert_to_numpy=True)[0]
        return tuple(vector.tolist())
    
    def generate_embedding(self, text: str, model_name: str = 'all-MiniLM-L6-v2') -> List[float]:
        """
        Generate an embedding for the given text.
        
        Args:
            text: Text to generate embedding for
            model_name: Name of the model to use (default: all-MiniLM-L6-v2)
            
        Returns:
            List of floats representing the embedding vector
            
        Raises:
            ModelNotFoundError: If model is not available
            EmbeddingError: If embedding generation fails
        """
        try:
            # Convert back from tuple to list
            return list(self._cached_generate_embedding(text, model_name))
        except ModelNotFoundError:
            raise
        except Exception as e:
            raise EmbeddingError(f"Failed to generate embedding: {str(e)}")
    
    def generate_batch_embeddings(self, texts: List[str], 
                                model_name: str = 'all-MiniLM-L6-v2') -> List[List[float]]:
        """
        Generate embeddings for multiple texts.
        
        Args:
            texts: List of texts to generate embeddings for
            model_name: Name of the model to use
            
        Returns:
            List of embedding vectors
            
        Raises:
            ModelNotFoundError: If model is not available
            EmbeddingError: If embedding generation fails
        """
        try:
            model = self.get_model(model_name)
            vectors = model.encode(texts, convert_to_numpy=True)
            return vectors.tolist()
        except ModelNotFoundError:
            raise
        except Exception as e:
            raise EmbeddingError(f"Failed to generate batch embeddings: {str(e)}")
    
    def get_embedding_dimension(self, model_name: str) -> int:
        """
        Get the dimension of embeddings generated by a model.
        
        Args:
            model_name: Name of the model
            
        Returns:
            Dimension of the embedding vectors
            
        Raises:
            ModelNotFoundError: If model is not available
        """
        if model_name in self.DEFAULT_MODELS:
            return self.DEFAULT_MODELS[model_name]['dimension']
        # If not in defaults, load model to get dimension
        model = self.get_model(model_name)
        return model.get_sentence_embedding_dimension()
    
    def clear_cache(self):
        """Clear both the model cache and the embedding cache."""
        self._model_cache.clear()
        self._cached_generate_embedding.cache_clear() 