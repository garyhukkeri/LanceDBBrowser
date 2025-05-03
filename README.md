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

```bash
pip install streamlit pandas
```

## Usage

```bash
streamlit run app.py
```

## Security Note

This application includes a password field for AWS Secret Access Key when connecting to remote databases. The credentials are stored in environment variables and are not persisted between sessions.

## License

Same as the original DesignerDataHub project.