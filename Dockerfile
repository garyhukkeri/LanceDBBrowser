FROM python:3.10-slim

WORKDIR /app

# Indicate that we're running in Docker
ENV RUNNING_IN_DOCKER=true

# Set default host database path (can be overridden at container runtime)
ENV HOST_DB_PATH=/Users/garyhukkeri/Library/CloudStorage/OneDrive-CVGlobal/Analytics\ Projects/Databot\ V2/ai-data-agent-dev/.lancedb

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose the Streamlit port
EXPOSE 8501

# Run the Streamlit application
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0", "--server.enableCORS=true", "--server.enableXsrfProtection=false"]
