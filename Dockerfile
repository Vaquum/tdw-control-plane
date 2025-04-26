FROM python:3.10-slim
RUN pip install --no-cache-dir dagster[redis,postgres] dagit clickhouse-driver jupyterlab lz4 clickhouse-cityhash polars pyarrow
ENV DAGSTER_HOME=/opt/trades-warehouse
WORKDIR /opt/trades-warehouse
COPY . /opt/trades-warehouse
