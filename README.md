# LanceDB Browser

A standalone Streamlit application for browsing and exploring LanceDB tables. This is a simplified fork of the DesignerDataHub project, focusing only on the LanceDB browser functionality.

## Pushing to GitHub

To push this repository to your own GitHub account:

1. Create a new repository on GitHub named "LanceDBBrowser"
2. Run the following commands:

```bash
# Add your GitHub repository as a remote
git remote add origin https://github.com/YOUR_USERNAME/LanceDBBrowser.git

# Push the code to your repository
git push -u origin master
```

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

## Security Note

This application includes a password field for AWS Secret Access Key when connecting to remote databases. The credentials are stored in environment variables and are not persisted between sessions.

## License

Same as the original DesignerDataHub project.