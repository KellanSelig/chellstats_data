FROM python:3.9-slim-buster as etl
WORKDIR .

COPY requirements.txt .
RUN pip install --no-cache-dir --force-reinstall --requirement requirements.txt

COPY etl ./etl
COPY util ./util
CMD uvicorn  etl.__main__:app --reload --port 8080 --host 0.0.0.0
