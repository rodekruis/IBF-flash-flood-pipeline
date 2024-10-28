from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import requests

from math import pi
from numpy import cos, sin
import rioxarray
from rasterio.enums import Resampling
import os
import json
from data_download.get_gauge_from_gmail import get_satellite_data
from data_download.process_compacted_iridium_data import (
    process_compacted_data,
)
from utils.general_utils.round_to_nearest_hour import (
    round_to_nearest_hour,
)
from settings.base import (
    HISTORIC_TIME_PERIOD_DAYS,
    WATERLEVEL_SENSOR,
    METEO_RAIN_SENSOR,
)

from itertools import compress


def tunnel_fast(latvar, lonvar, lat0, lon0):
    """
    Find closest point in a set of (lat,lon) points to specified point
    latvar - 2D latitude variable from an open netCDF dataset
    lonvar - 2D longitude variable from an open netCDF dataset
    lat0,lon0 - query point
    Returns iy,ix such that the square of the tunnel distance
    between (latval[it,ix],lonval[iy,ix]) and (lat0,lon0)
    is minimum.
    """
    rad_factor = pi / 180.0  # for trignometry, need angles in radians
    # Read latitude and longitude from file into numpy arrays
    latvals = latvar[:] * rad_factor
    lonvals = lonvar[:] * rad_factor
    ny, nx = latvals.shape
    lat0_rad = lat0 * rad_factor
    lon0_rad = lon0 * rad_factor
    # Compute numpy arrays for all values, no loops
    clat, clon = cos(latvals), cos(lonvals)
    slat, slon = sin(latvals), sin(lonvals)
    delX = cos(lat0_rad) * cos(lon0_rad) - clat * clon
    delY = cos(lat0_rad) * sin(lon0_rad) - clat * slon
    delZ = sin(lat0_rad) - slat
    dist_sq = delX**2 + delY**2 + delZ**2
    minindex_1d = dist_sq.argmin()  # 1D index of minimum element
    iy_min, ix_min = np.unravel_index(minindex_1d, latvals.shape)
    return iy_min, ix_min


class dataGetter:
    def __init__(self, ta_gdf):
        self.ta_gdf = ta_gdf

    def gather_satellite_data(self):
        filename_list = get_satellite_data()
        for filename in filename_list:
            with open(filename, "br") as file:
                data = file.read()
                process_compacted_data(r"data/gauge_data", data, len(data), "Karonga")

    def get_sensor_values(self):
        gauges_data_files = os.listdir("data/gauge_data")
        month = datetime.now().month
        gauges_actual_data_dict = {}
        gauges_reference_value_dict = {}
        gauges_yesterday_dict = {}
        yesterday = datetime.now() - timedelta(days=1)

        karonga_sensor_data_files = [x for x in gauges_data_files if x.endswith(".txt")]
        datetime_list = [
            datetime.strptime(x.split("_")[1], "%y%m%d%H%M%S.txt")
            for x in karonga_sensor_data_files
        ]
        karonga_latest_file = karonga_sensor_data_files[-1]
        karonga_file_yesterday = (
            "data/gauge_data/Karonga_"
            + datetime.strftime(
                min(datetime_list, key=lambda d: abs(d - yesterday)), "%y%m%d%H%M%S"
            )
            + ".txt"
        )
        reference_values = pd.read_csv("data/gauge_data/sensor_info_karonga.csv")
        fid = reference_values.loc[reference_values["key"] == "fid", "value"].item()
        gauges_reference_value_dict[fid] = reference_values.loc[
            reference_values["key"] == str(month), "value"
        ].item()
        karonga_yesterday = pd.read_csv(
            karonga_file_yesterday,
            delimiter=";",
            skiprows=1,
            usecols=[1, 3],
            header=None,
            names=["time", "distance"],
        )

        gauges_yesterday_dict[fid] = (
            karonga_yesterday["distance"].iloc[-1]
            + reference_values.loc[
                reference_values["key"] == "elevation", "value"
            ].item()
            + reference_values.loc[
                reference_values["key"] == "sensor_height", "value"
            ].item()
        )

        karonga_today = pd.read_csv(
            "data/gauge_data/" + karonga_latest_file,
            delimiter=";",
            skiprows=1,
            usecols=[1, 3],
            header=None,
            names=["time", "distance"],
        )
        gauges_actual_data_dict[fid] = (
            karonga_today["distance"].iloc[-1]
            + reference_values.loc[
                reference_values["key"] == "elevation", "value"
            ].item()
            + reference_values.loc[
                reference_values["key"] == "sensor_height", "value"
            ].item()
        )

        for sensor in WATERLEVEL_SENSOR:
            sensor_filtered_list = [
                x for x in gauges_data_files if x.endswith(str(sensor) + ".json")
            ]
            datetime_list = [
                datetime.strptime(x.split("_")[0], "%Y%m%d%H%M%S")
                for x in sensor_filtered_list
            ]
            file_yesterday = (
                "data/gauge_data/"
                + datetime.strftime(
                    min(datetime_list, key=lambda d: abs(d - yesterday)), "%Y%m%d%H%M%S"
                )
                + "_"
                + str(sensor)
                + ".json"
            )
            data_file_today = sensor_filtered_list[-1]
            with open("data/gauge_data/" + data_file_today) as json_file:
                data = json.load(json_file)
            last_non_empty = [i for i in data["data"] if "Wlev" in i][-1]
            reference_values = pd.read_csv(
                "data/gauge_data/sensor_info_{}.csv".format(str(sensor))
            )
            fid = reference_values.loc[reference_values["key"] == "fid", "value"].item()
            if isinstance(last_non_empty["Wlev"], str):
                waterlevel = float(last_non_empty["Wlev"].strip("*T"))
            else:
                waterlevel = float(last_non_empty["Wlev"])
            gauges_actual_data_dict[fid] = (
                waterlevel
                + reference_values.loc[
                    reference_values["key"] == "elevation", "value"
                ].item()
                + reference_values.loc[
                    reference_values["key"] == "sensor_height", "value"
                ].item()
            )
            gauges_reference_value_dict[fid] = reference_values.loc[
                reference_values["key"] == str(month), "value"
            ].item()
            with open(file_yesterday) as json_file:
                data = json.load(json_file)
            last_non_empty_yesterday = [i for i in data["data"] if "Wlev" in i][-1]
            if isinstance(last_non_empty_yesterday["Wlev"], str):
                waterlevel = float(last_non_empty_yesterday["Wlev"].strip("*T"))
            else:
                waterlevel = float(last_non_empty_yesterday["Wlev"])
            gauges_yesterday_dict[fid] = (
                waterlevel
                + reference_values.loc[
                    reference_values["key"] == "elevation", "value"
                ].item()
                + reference_values.loc[
                    reference_values["key"] == "sensor_height", "value"
                ].item()
            )
        return (
            gauges_actual_data_dict,
            gauges_reference_value_dict,
            gauges_yesterday_dict,
        )

    def get_rain_gauge(self):
        gauges_data_files = os.listdir("data/gauge_data")
        rain_start_date = datetime.now() - timedelta(days=60)

        sensor_filtered_list = [
            x for x in gauges_data_files if x.endswith(str(METEO_RAIN_SENSOR) + ".json")
        ]
        datetime_list = [
            datetime.strptime(x.split("_")[0], "%Y%m%d%H%M%S")
            for x in sensor_filtered_list
        ]
        relevant_files = [x > rain_start_date for x in datetime_list]
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
            rain_df = pd.DataFrame(
                rain_timeseries, columns=["datetime", "precipitation"]
            )
            temp = rain_df.diff()
            rain_df["precipitation"] = temp["precipitation"]
        else:
            rain_df = None

        return rain_df

    def get_rain_forecast(self):
        gfs_data = {}
        # historic_start = round_to_nearest_hour(datetime.now()) - timedelta(
        #     days=2, hours=3
        # )
        # historic_date_list = [
        #     historic_start + timedelta(hours=3 * x) for x in range(18)
        # ]
        forecast_start = round_to_nearest_hour(datetime.now()) - timedelta(hours=9)
        # forecast_date_list = [
        #     forecast_start + timedelta(hours=3 * x) for x in range(30)
        # ]
        forecast_start_hour = round(forecast_start.hour / 6) * 6
        if forecast_start_hour < 12:
            forecast_start_hour = "0" + str(forecast_start_hour)
        if forecast_start_hour == 24:
            forecast_start_hour = "00"
            forecast_start = forecast_start + timedelta(days=1)
        # nc_dataset_historic = nc.Dataset(
        # "https://nomads.ncep.noaa.gov/dods/gfs_0p25/gfs{}/gfs_0p25_00z".format(
        # historic_start.strftime("%Y%m%d")
        # )
        # )
        # nc_dataset_forecast = nc.Dataset(
        # "https://nomads.ncep.noaa.gov/dods/gfs_0p25/gfs{}/gfs_0p25_{}z".format(
        # forecast_start.strftime("%Y%m%d"), forecast_start_hour
        # )
        # )
        # latvar = nc_dataset_historic.variables["lat"][:]
        # lat_dim = len(latvar)
        # lonvar = nc_dataset_historic.variables["lon"][:]
        # lon_dim = len(lonvar)
        # latvar = np.stack([latvar for _ in range(lon_dim)], axis=0)
        # lonvar = np.stack([lonvar for _ in range(lat_dim)], axis=1)
        ta_gdf_4326 = self.ta_gdf.copy()
        ta_gdf_4326.to_crs(4326, inplace=True)
        now = datetime.now()

        xds = rioxarray.open_rasterio(
            r"data/cosmo/COSMO_MLW_{}T00_prec.nc".format(now.strftime("%Y%m%d"))
        )
        xds.rio.write_crs("epsg:4326", inplace=True)

        upscale_factor = 8
        new_width = xds.rio.width * upscale_factor
        new_height = xds.rio.height * upscale_factor
        xds_upsampled_forecast = xds.rio.reproject(
            xds.rio.crs,
            shape=(new_height, new_width),
            resampling=Resampling.bilinear,
        )
        datetime_list_forecast = [
            datetime.strptime(x.isoformat(), "%Y-%m-%dT%H:%M:%S")
            for x in xds_upsampled_forecast.time.data[:]
        ]

        past = now - timedelta(days=5)
        xds = rioxarray.open_rasterio(
            r"data/cosmo/COSMO_MLW_{}T00_prec.nc".format(past.strftime("%Y%m%d"))
        )
        xds.rio.write_crs("epsg:4326", inplace=True)

        xds_upsampled_hindcast = xds.rio.reproject(
            xds.rio.crs,
            shape=(new_height, new_width),
            resampling=Resampling.bilinear,
        )
        datetime_list_hindcast = [
            datetime.strptime(x.isoformat(), "%Y-%m-%dT%H:%M:%S")
            for x in xds_upsampled_hindcast.time.data[:]
        ]

        for _, row in ta_gdf_4326.iterrows():
            xds_clipped = xds_upsampled_hindcast.rio.clip(
                [row["geometry"]], ta_gdf_4326.crs
            )
            xds_clipped.data[xds_clipped.data > 1000] = np.nan
            cum_mean_rain_ts = [np.nanmean(x) for x in xds_clipped.data]
            mean_rain_ts = [cum_mean_rain_ts[0]]
            for x in range(1, len(cum_mean_rain_ts)):
                mean_rain_ts.append(cum_mean_rain_ts[x] - cum_mean_rain_ts[x - 1])
            gfs_data[row["placeCode"]] = pd.DataFrame(
                {"datetime": datetime_list_hindcast, "precipitation": mean_rain_ts}
            )

            xds_clipped = xds_upsampled_forecast.rio.clip(
                [row["geometry"]], ta_gdf_4326.crs
            )
            xds_clipped.data[xds_clipped.data > 1000] = np.nan
            cum_mean_rain_ts = [np.nanmean(x) for x in xds_clipped.data]
            mean_rain_ts = [cum_mean_rain_ts[0]]
            for x in range(1, len(cum_mean_rain_ts)):
                mean_rain_ts.append(cum_mean_rain_ts[x] - cum_mean_rain_ts[x - 1])
            gfs_data[row["placeCode"]] = pd.DataFrame(
                {"datetime": datetime_list_forecast, "precipitation": mean_rain_ts}
            )
            gfs_data[row["placeCode"]] = gfs_data[row["placeCode"]].drop_duplicates(
                subset="datetime", keep="last"
            )
        return gfs_data
