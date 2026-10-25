FROM python:3.11-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /workspace
COPY requirements.txt requirements-spark.txt /tmp/requirements/
RUN pip install --no-cache-dir -r /tmp/requirements/requirements-spark.txt
ENV PYTHONPATH=/workspace/src \
    PYSPARK_PYTHON=/usr/local/bin/python \
    SPARK_LOCAL_IP=127.0.0.1 \
    PYTHONUNBUFFERED=1
COPY src/ /workspace/src/
CMD ["python", "-m", "freewaypulse.platform", "check"]
