# Basic isolated python environment.
FROM python:3.11-slim

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y libgdal-dev libeccodes-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
    
RUN pip install GDAL==3.8.4
WORKDIR /code
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD python flash_flood_pipeline/runPipeline.py
