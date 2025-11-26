FROM python:3.10-slim

ENV PIP_DEFAULT_TIMEOUT=100

RUN apt-get update && apt-get install -y git openssh-client libgomp1

RUN mkdir -p /root/.ssh && \
    chmod 700 /root/.ssh && \
    touch /root/.ssh/known_hosts && \
    ssh-keyscan -t rsa github.com >> /root/.ssh/known_hosts && \
    chmod 644 /root/.ssh/known_hosts

COPY docker-requirements.txt .
RUN pip install --no-cache-dir -r docker-requirements.txt

RUN --mount=type=ssh,id=trades-warehouse \
    echo "========= DETAILED SSH DEBUG =========" && \
    echo "1. SSH directory:" && \
    ls -la /root/.ssh/ && \
    echo "2. SSH Agent information:" && \
    echo "SSH_AUTH_SOCK: $SSH_AUTH_SOCK" && \
    echo "3. Known hosts content:" && \
    cat /root/.ssh/known_hosts && \
    echo "4. Testing direct GitHub connection:" && \
    ssh -o StrictHostKeyChecking=no -vT git@github.com 2>&1 || echo "Exit code: $?" && \
    echo "5. SSH agent identities:" && \
    ssh-add -l 2>&1 || echo "Exit code: $?" && \
    echo "========= END DEBUG ========="

#RUN --mount=type=ssh,id=trades-warehouse \
#    GIT_SSH_COMMAND="ssh -o StrictHostKeyChecking=no -v" \
#    pip install --no-cache-dir --upgrade git+ssh://git@github.com/vaquum/Loop.git

ENV DAGSTER_HOME=/opt/trades-warehouse
WORKDIR /opt/trades-warehouse
COPY . /opt/trades-warehouse
