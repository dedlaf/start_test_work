FROM python:3.9-slim

WORKDIR /app

COPY main.py /app
COPY requirements.txt /app

RUN pip install -r requirements.txt

ENTRYPOINT ["python3", "main.py", "consume", "--topic", "hello_topic", "--kafka", "kafka:29092"]
