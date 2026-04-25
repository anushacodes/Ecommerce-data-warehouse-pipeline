FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install uv for fast dependency resolution
RUN pip install uv

# Copy dependency definitions
COPY pyproject.toml ./

# Install dependencies using uv into the system Python
RUN uv pip install --system -r pyproject.toml

# Copy the rest of the application
COPY . .

# Expose the dashboard port
EXPOSE 8050

# Default command to run the dashboard
CMD ["python", "dashboard.py"]
