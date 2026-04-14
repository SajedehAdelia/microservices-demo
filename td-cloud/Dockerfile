FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV PORT=8080
# Use eventlet worker for WebSocket support
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--worker-class", "eventlet", "--workers", "1", "main:app"]