import sys

sys.path.append(r"D:\VSCode\IBF-flash-flood-pipeline\flash_flood_pipeline")

from datetime import datetime, timedelta
import pandas as pd
import geopandas as gpd
import os
import json
from settings.base import BLANTYRE_RAINFALL_SENSORS, METEO_RAIN_SENSOR, ENVIRONMENT
from itertools import compress
import logging
from pathlib import Path
import numpy as np
logger = logging.getLogger(__name__)


def process_karonga_rainfall_sensor_data(start_date):
    gauges_data_files = os.listdir("data/gauge_data")
    rain_start_date = start_date  # datetime.now() - timedelta(days=60)

    sensor_filtered_list = [
        x for x in gauges_data_files if x.endswith(str(METEO_RAIN_SENSOR) + ".json")
    ]
    datetime_list = [
        datetime.strptime(x.split("_")[0], "%Y%m%d%H%M%S") for x in sensor_filtered_list
    ]

    relevant_files = [x for x in datetime_list if x > rain_start_date]
    logger.info(f"Karonga relevant files: {relevant_files}")
    if relevant_files:
        rain_files = list(compress(sensor_filtered_list, relevant_files))
        rain_timeseries = []
        for file in rain_files:
            with open("data/gauge_data/" + file) as json_file:
                data = json.load(json_file)
                rain_timeseries += [
                    [datetime.strptime(str(x["$ts"]), "%y%m%d%H%M%S"), x["Rain"]]
                    for x in data["data"]
                    if "Rain" in x
                ]
        rain_df = pd.DataFrame(rain_timeseries, columns=["datetime", "precipitation"])
        temp = rain_df.diff()
        rain_df["precipitation"] = temp["precipitation"]
    else:
        rain_df = None

    return rain_df


def process_blantyre_rainfall_sensor_data():
    start_date = datetime.now()
    archive_location = Path(r"data/gauge_data")

    start_sensor_date = start_date - timedelta(days=7)

    rain_sensor_data_collection = []

    for gauge_name, gauge_id in BLANTYRE_RAINFALL_SENSORS.items():
        if gauge_name != "blantyre_waterboard_reservoir":
            gauge_files = [fn for fn in archive_location.glob(f"*{gauge_id}.json")]

            dataframe_entry_list = []

            for gauge_file in gauge_files:
                with open(gauge_file, "r") as src:
                    f = json.load(src)
                    rainfall_data = [
                        data_entry for data_entry in f["data"] if "Rain" in data_entry
                    ]

                    for rainfall_ts in rainfall_data:
                        dataframe_entry_list.append(
                            pd.Series(
                                data={
                                    "rainfall": rainfall_ts["Rain"],
                                },
                                name=datetime.strptime(
                                    str(rainfall_ts["$ts"]), "%y%m%d%H%M%S"
                                ),
                            )
                        )

            sensor_data = pd.concat(dataframe_entry_list, axis=1).T

            sensor_data = sensor_data.reset_index()
            sensor_data = sensor_data.drop_duplicates().set_index("index")

            for col in sensor_data.columns:
                sensor_data[col] = pd.to_numeric(sensor_data[col], errors="coerce")

            sensor_data = sensor_data.sort_index()
            sensor_data = sensor_data.rename(columns={"rainfall": f"{gauge_name}_rain"})
            sensor_data = sensor_data.loc[sensor_data.index >= start_sensor_date]
            rain_sensor_data_collection.append(sensor_data)

    gauge_rainfall = pd.concat(rain_sensor_data_collection, axis=1)
    gauge_rainfall = gauge_rainfall.diff()
    gauge_rainfall = gauge_rainfall.fillna(0)
    gauge_rainfall = gauge_rainfall.resample("h").sum()
    return gauge_rainfall


def apply_idw(ta_centroid, ta_name, gauge_locations_gdf, gauge_timeseries, p=2):
    gauge_meta = gauge_locations_gdf.copy()
    idw_gauge_timeseries = gauge_timeseries.copy()
    gauge_meta["dist"] = gauge_meta.apply(
        lambda row: row.geometry.distance(ta_centroid), axis=1
    )

    gauge_meta["weight"] = gauge_meta.apply(
        lambda row: (1 / row.dist**p)
        / np.sum([(1 / g_dst**p) for g_dst in gauge_meta["dist"].tolist()]),
        axis=1,
    )
    idw_weight_mapping = {
        k: v for k, v in zip(gauge_meta["name"].tolist(), gauge_meta["weight"].tolist())
    }

    idw_gauge_timeseries[f"{ta_name}"] = idw_gauge_timeseries.apply(
        lambda row: np.sum([row[g] * idw_weight_mapping.get(g.split("_rain")[0]) for g in row.index if g != "index"]),
        axis=1,
    )
    ta_timeseries_idw = idw_gauge_timeseries[[f"{ta_name}"]].copy()
    ta_timeseries_idw = ta_timeseries_idw.fillna(0)
    return ta_timeseries_idw


def blantyre_raingauge_idw(
    ta_gdf,
    sensor_data_df,
    single_gauge_distance_threshold=1000,
    triple_gauge_distance_threshold=5000,
):
    ta_gdf_32736 = ta_gdf.copy().to_crs(epsg=32736)

    blantyre_gauge_locations = gpd.read_file(
        Path(rf"data\static_data\{ENVIRONMENT}\installed_sensor_locations.gpkg")
    ).to_crs(epsg=32736)

    def eligible_for_idw(row):
        gauges_within_sgt = []
        gauges_within_tgt = []

        for _, gauge in blantyre_gauge_locations.iterrows():
            if row.geometry.centroid.distance(
                gauge.geometry
            ) < single_gauge_distance_threshold or gauge.geometry.within(row.geometry):
                gauges_within_sgt.append(gauge["name"])
            if (
                row.geometry.centroid.distance(gauge.geometry)
                < triple_gauge_distance_threshold
            ):
                gauges_within_tgt.append(gauge["name"])

        if len(gauges_within_sgt) >= 1 or len(gauges_within_tgt) >= 3:
            return True
        else:
            return False

    ta_gdf_32736["idw_eligibility"] = ta_gdf_32736.apply(
        lambda row: eligible_for_idw(row), axis=1
    )

    ta_gdf_32736_idw = ta_gdf_32736.loc[ta_gdf_32736["idw_eligibility"]]

    idw_ts_collection = []

    for _, row in ta_gdf_32736_idw.iterrows():
        idw_ts = apply_idw(
            ta_centroid=row.geometry.centroid,
            ta_name=row.placeCode,
            gauge_locations_gdf=blantyre_gauge_locations,
            gauge_timeseries=sensor_data_df,
        )
        
        idw_ts_collection.append(idw_ts)

    idw_timeseries_combined = pd.concat(idw_ts_collection, axis=1)
    return idw_timeseries_combined



if __name__ == "__main__":
    ta_gdf = gpd.read_file(
        r"d:\VSCode\IBF-flash-flood-pipeline\data\static_data\dev\regions.gpkg"
    )
    sensor_data_df = pd.read_csv(
        r"d:\VSCode\IBF-flash-flood-pipeline\data\dev\debug_output\blantyre_sensors_ts_2025-01-24_14.csv"
    )

    df = blantyre_raingauge_idw(ta_gdf, sensor_data_df)
    print(df)
