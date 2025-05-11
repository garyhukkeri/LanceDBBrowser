# LanceDB Browser

A standalone Streamlit application for browsing and exploring LanceDB tables. This is a simplified fork of the DesignerDataHub project, focusing only on the LanceDB browser functionality.

## Features

- Connect to local or remote LanceDB databases
- Browse and explore tables
- View table schema and data
- Execute queries
- Create new tables
- Delete rows
- Generate embeddings from table data
  - Select multiple fields to combine for embedding generation
  - Choose from different embedding models
  - Automatically create vector columns for semantic search
- Perform semantic search on vector data
- Manage table operations (create, delete, replace)

## Installation

### Standard Installation

```bash
pip install streamlit pandas sentence-transformers lancedb
```

### Docker Installation

```bash
docker-compose up -d
```

## Usage

### Standard Usage

```bash
# Run with default settings
streamlit run app.py

# Or use the provided script
./run.sh
```

### Docker Usage
1. Build and run with Docker Compose:
```bash
# Start the container
docker-compose up -d

# Access the application at http://localhost:8501
```
2. Or run with Docker directly, providing the host database path:

```bash
docker build -t lancedb-browser .
docker run -p 8501:8501 -e HOST_DB_PATH=/path/to/db -v /path/to/your/host/database:/path/to/db lancedb-browser

# Access the application at http://localhost:8501
```

## Features in Detail

### Table Management
- Create new tables from CSV or Parquet files
- View and explore table contents
- Delete tables when needed
- Replace existing tables with updated data

### Embedding Generation
- Select one or more fields to generate embeddings from
- Combine text from multiple fields for richer embeddings
- Choose from available embedding models:
  - all-MiniLM-L6-v2 (default)
  - More models coming soon
- Automatically handle table updates when adding embeddings

### Semantic Search
- Search through vector data using semantic similarity
- Utilize generated embeddings for finding similar content
- Get relevance scores for search results

## Architecture

The application is built with a service-oriented architecture:
- `LanceDBService`: Core database operations
- `TableOperationsService`: Table management and operations
- `EmbeddingService`: Handles embedding generation
- Streamlit UI: User interface and interaction

## Contributing

Feel free to open issues or submit pull requests at [GitHub Repository](https://github.com/garyhukkeri/LanceDBBrowser)

## License

This project is licensed under the MIT License - see the LICENSE file for details.