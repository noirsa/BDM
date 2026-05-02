FROM quay.io/jupyter/pyspark-notebook:spark-4.1.1

USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*

USER ${NB_UID}

COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

ENV PYSPARK_PYTHON=/usr/bin/python3.13
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3.13

WORKDIR /home/jovyan