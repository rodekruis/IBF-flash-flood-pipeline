# Basic isolated python environment.
FROM python:3.10

RUN apt-get update

RUN apt-get install -y libgdal-dev

RUN pip install GDAL==3.2.2.1
WORKDIR /code
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD python flash_flood_pipeline/runPipeline.py
