# Basic isolated python environment.
FROM python:3.11-slim

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y --no-install-recommends \
        build-essential \
        g++ \
        libgdal-dev \
        libeccodes-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir "GDAL==$(gdal-config --version)"
WORKDIR /code
COPY requirements.txt ./
RUN pip install --no-cache-dir setuptools wheel "Cython~=3.0.2" "numpy==1.26.4" && \
    pip install --no-cache-dir --no-build-isolation --no-binary=fiona,rasterio -r requirements.txt
COPY . .
CMD python flash_flood_pipeline/runPipeline.py
