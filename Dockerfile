# Dockerfile for dlsidecar
# Single-stage Python build — no Node needed (Streamlit replaces React)

FROM python:3.12-slim AS runtime

# System deps for DuckDB, Arrow, Thrift
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ libffi-dev libssl-dev curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv for fast package management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Copy project files
COPY pyproject.toml ./
COPY src/ ./src/

# Install Python dependencies (includes streamlit)
RUN uv pip install --system -e ".[all]"

# Pre-install DuckDB extensions at build time
RUN python -c "\
import duckdb; \
conn = duckdb.connect(':memory:'); \
for ext in ['httpfs', 'iceberg', 'json', 'parquet']: \
    try: conn.execute(f\"INSTALL '{ext}'\"); print(f'Installed {ext}') \
    except: print(f'Skipped {ext}'); \
conn.close()"

# Create non-root user
RUN useradd -m -s /bin/bash dlsidecar
USER dlsidecar

# Data directory for persistent DuckDB mode
RUN mkdir -p /home/dlsidecar/data

# Expose ports: REST, Flight, Metrics, Streamlit UI
EXPOSE 8765 8766 9090 3000

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8765/healthz || exit 1

# Default command
CMD ["python", "-m", "dlsidecar.main"]
