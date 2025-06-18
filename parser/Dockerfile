# /parser/Dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY parser.py .

RUN pip install pika

CMD ["python", "parser.py"]
