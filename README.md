# LanceDB Browser

A standalone Streamlit application for browsing and exploring LanceDB tables. This is a simplified fork of the DesignerDataHub project, focusing only on the LanceDB browser functionality.

## Features

- Connect to local or remote LanceDB databases
- Browse and explore tables
- View table schema and data
- Execute queries
- Create new tables
- Delete rows

## Installation

### Standard Installation

```bash
pip install streamlit pandas
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
1. Or run with Docker directly, providing the host database path:

```bash
docker build -t lancedb-browser .
docker run -p 8501:8501 -e HOST_DB_PATH=/path/to/db -v /path/to/your/host/database:/path/to/db lancedb-browser

# Access the application at http://localhost:8501
```