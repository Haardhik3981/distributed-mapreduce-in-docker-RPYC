FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY worker.py .
ENV RPYC_PORT=18861
EXPOSE 18861
CMD ["python", "worker.py"]
