FROM python:3.10-slim
COPY docker-requirements.txt .
RUN pip install --no-cache-dir -r docker-requirements.txt
ENV DAGSTER_HOME=/opt/trades-warehouse
WORKDIR /opt/trades-warehouse
COPY . /opt/trades-warehouse
