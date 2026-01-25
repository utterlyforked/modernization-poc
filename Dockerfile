FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app

# Install dependencies
RUN pip install flask flask-cors --no-cache-dir

# Copy all application files
COPY setup_legacy_a.py .
COPY setup_legacy_b.py .
COPY setup_new_system.py .
COPY migrate_add_modernized_only.py .
COPY sync_adapter.py .
COPY api.py .
COPY index.html .
COPY start.sh .

# Make start script executable
RUN chmod +x start.sh

# Expose ports
EXPOSE 5000 8000

# Run the start script
CMD ["./start.sh"]
