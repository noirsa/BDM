FROM apache/airflow:3.1.7

# Additional packages required by the lab DAGs
# (installed as the airflow user – no root needed)
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt