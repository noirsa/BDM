FROM python:3.10

WORKDIR /app
COPY producer.py .

RUN pip install kafka-python requests

CMD ["python", "producer.py"]