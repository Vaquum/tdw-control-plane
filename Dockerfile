FROM python:3.10-slim

ENV PIP_DEFAULT_TIMEOUT=100

RUN apt-get update && apt-get install -y git openssh-client libgomp1 python3-venv

COPY docker-requirements.txt .
RUN pip install --no-cache-dir -r docker-requirements.txt

ENV DAGSTER_HOME=/opt/trades-warehouse
WORKDIR /opt/trades-warehouse
COPY . /opt/trades-warehouse
