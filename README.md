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

```bash
# Start the container
docker-compose up -d

# Access the application at http://localhost:8501
```