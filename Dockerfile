FROM python:3.9-slim-buster as etl
WORKDIR .

COPY requirements.txt .
RUN pip install --no-cache-dir --force-reinstall --requirement requirements.txt

COPY etl ./etl
COPY util ./util
ENTRYPOINT ["python", "-m", "etl"]
