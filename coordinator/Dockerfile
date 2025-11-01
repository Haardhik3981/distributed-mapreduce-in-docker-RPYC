FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY coordinator.py .
# place for data
RUN mkdir -p /data/txt
ENV DATA_DIR=/data/txt RPYC_PORT=18861 TASK_TIMEOUT_SECS=20 NUM_REDUCERS=3
CMD ["python", "coordinator.py"]
