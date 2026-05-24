FROM spark:4.1.1-scala2.13-java17-ubuntu

USER root

RUN set -ex; \
    apt-get update; \
    apt-get install -y software-properties-common curl gnupg; \
    add-apt-repository ppa:deadsnakes/ppa; \
    apt-get update; \
    apt-get install -y \
        python3.13 \
        python3.13-dev \
        python3.13-venv \
        curl; \
    rm -rf /var/lib/apt/lists/*

# install pip explicitly (THIS IS THE FIX)
RUN curl -sS https://bootstrap.pypa.io/get-pip.py | python3.13

# install python packages using correct interpreter
RUN python3.13 -m pip install --no-cache-dir \
    boto3 \
    pillow \
    numpy \
    pandas \
    torch \
    transformers \
    pymilvus \
    clickhouse_connect

ENV PYSPARK_PYTHON=/usr/bin/python3.13
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3.13

RUN curl -L -o /opt/spark/jars/postgresql.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

USER spark