# BIENES — production container for Coolify
FROM python:3.12-slim

# Set up app directory
WORKDIR /app

# Install Python deps first (better layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY *.py ./
COPY tools/ ./tools/

# Output dir is mounted as a volume in production for cache + history
RUN mkdir -p /app/output

EXPOSE 5055

# Gunicorn config:
#   -w 1: single worker (RUN_STATE is in-memory; multi-worker would lose it)
#   --timeout 600: pipeline subprocess can take 60-120s; bump worker timeout
#   --threads 4: handle concurrent /status polls while pipeline runs
#   --access-logfile -: stream logs to stdout (Coolify captures these)
CMD ["gunicorn", "-w", "1", "--threads", "4", "--timeout", "600", \
     "-b", "0.0.0.0:5055", "--access-logfile", "-", "gui:app"]
