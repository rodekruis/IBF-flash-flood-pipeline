# IBF-flash-flood-pipeline

Forecast Flash Floods. Part of [IBF-system](https://github.com/rodekruis/IBF-system).

## Description

The pipeline roughly consists of three steps:
* Extract data on measured and forecasted rainfall from external providers. For Malawi GFS (API), COSMO (local weather forecast, Azure), rain gauges and water level sensors (both Azure) are imported. 
* Forecast floods by determining the corresponding flood maps for a certain amount of cumulative rainfall in a given time period; A library of precomputed flood maps should be created/configured before deploying the pipeline.
* Send this data to the IBF app.

The pipeline has a library of flood maps and archive of sensor measurement data in:
* [ibf-file share](https://510ibfsystem.file.core.windows.net/rhdhv-flashflood-mwi) (Azure File Share)

The pipeline depends on the following services:
* [COSMO](https://www.cosmo-model.org/content/default.htm): provides high resolution rainfall forecasts 
* [Sensor data ingestion API](https://ibf-sensor-data-ingestion.azure-api.net) (Azure API Management service): Receives sensor data (json) and sends it to logic app:
* [Sensor data ingestion logic app](https://portal.azure.com/#@rodekruis.onmicrosoft.com/resource/subscriptions/57b0d17a-5429-4dbb-8366-35c928e3ed94/resourceGroups/IBF-system/providers/Microsoft.Logic/workflows/rhdhv-ibf-sensor-data-ingestion/logicApp) (Azure Logic app) extracts information from JSON body and stores it in the Azure file share
* IBF-app 

For more information, see the [functional architecture diagram](https://miro.com/app/board/uXjVK7Valso=/?moveToWidget=3458764592859255828&cot=14).

## Basic Usage

To run the pipeline locally
1. fill in the secrets in `credentials.py`
2. install requirements
```
pip install poetry
poetry install --no-interaction
```
3. run the pipeline with `python flash_flood_pipeline/runPipeline.py`
