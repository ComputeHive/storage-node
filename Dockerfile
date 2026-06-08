FROM python:3.12-slim

LABEL maintainer="storage-host-node"
LABEL description="Decentralized storage host node"

RUN useradd --create-home --shell /bin/bash nodeuser

WORKDIR /home/nodeuser/app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /home/nodeuser/app/Data \
    && mkdir -p /home/nodeuser/app/Cache \
    && chown -R nodeuser:nodeuser /home/nodeuser/app

USER nodeuser

EXPOSE 50000-60000

HEALTHCHECK --interval=5m --timeout=10s --retries=3 \
    CMD python -c "import httpx; httpx.get('https://api.ipify.org', timeout=5)" || exit 1

ENTRYPOINT ["python", "storage_node.py"]