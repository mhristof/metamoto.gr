FROM python:3.13.1

RUN pip install --no-cache-dir flask flask-cors clickhouse-connect waitress

COPY ./app.py /app.py

CMD ["python", "/app.py"]

