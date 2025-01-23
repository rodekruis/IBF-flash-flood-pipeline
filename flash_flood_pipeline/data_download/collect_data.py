from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import netCDF4 as nc
from pathlib import Path
import xarray as xr
import rioxarray
from rasterio.enums import Resampling
import os
import json
from data_download.download_gpm import GpmDownload
from data_download.get_gauge_from_gmail import get_satellite_data
from data_download.utils.tunnel_fast import tunnel_fast
from data_download.utils.extract_lat_lon import extract_lat_lon
from flash_flood_pipeline.data_processing.process_compacted_iridium_data import (
    process_compacted_data,
)
from utils.general_utils.round_to_nearest_hour import (
    round_to_nearest_hour,
)
from settings.base import (
    WATERLEVEL_SENSOR,
    METEO_RAIN_SENSOR,
)
from itertools import compress
import logging

logger = logging.getLogger(__name__)


class dataGetter:
    def __init__(self, ta_gdf):
        self.ta_gdf = ta_gdf
        self.malawi_bbox = (
            31.0000000000000000,  # lon min
            -19.0000000000000000,  # lat min
            38.0000000000000000,  # lon max
            -7.0000000000000000,  # lat max
        )

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
        fid = int(
            reference_values.loc[reference_values["key"] == "fid", "value"].item()
        )

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
        now = datetime.now()
        past = now - timedelta(days=5)

        expected_cosmo_hindcast_path = Path(
            r"data/cosmo/COSMO_MLW_{}T00_prec.nc".format(past.strftime("%Y%m%d"))
        )

        expected_cosmo_forecast_path = Path(
            r"data/cosmo/COSMO_MLW_{}T00_prec.nc".format(now.strftime("%Y%m%d"))
        )

        gfs_data = {}

        if (
            not expected_cosmo_forecast_path.exists()
            or not expected_cosmo_hindcast_path.exists()
        ):
            # switch to gfs
            logger.info("COSMO-data not found, switching to GFS-data")
            hindcast_start = round_to_nearest_hour(datetime.now()) - timedelta(
                days=2, hours=3
            )

            forecast_start = round_to_nearest_hour(datetime.now()) - timedelta(hours=9)

            forecast_start_hour = round(forecast_start.hour / 6) * 6

            if forecast_start_hour < 12:
                forecast_start_hour = "0" + str(forecast_start_hour)

            if forecast_start_hour == 24:
                forecast_start_hour = "00"
                forecast_start = forecast_start + timedelta(days=1)

            nc_dataset_hindcast = nc.Dataset(
                "https://nomads.ncep.noaa.gov/dods/gfs_0p25/gfs{}/gfs_0p25_00z".format(
                    hindcast_start.strftime("%Y%m%d")
                )
            )

            nc_dataset_forecast = nc.Dataset(
                "https://nomads.ncep.noaa.gov/dods/gfs_0p25/gfs{}/gfs_0p25_{}z".format(
                    forecast_start.strftime("%Y%m%d"), forecast_start_hour
                )
            )
            # xr_dataset_hindcast = xr.open_dataset(
            #     xr.backends.NetCDF4DataStore(nc_dataset_hindcast)
            # )
            # xr_dataset_hindcast = xr_dataset_hindcast.rio.set_spatial_dims("lon", "lat")
            # xr_dataset_hindcast = xr_dataset_hindcast.rio.write_crs("epsg:4326")

            # xr_dataset_hindcast = xr_dataset_hindcast.rio.clip_box(*self.malawi_bbox)

            # xr_dataset_forecast = xr.open_dataset(
            #     xr.backends.NetCDF4DataStore(nc_dataset_forecast)
            # )
            # xr_dataset_forecast = xr_dataset_forecast.rio.set_spatial_dims("lon", "lat")
            # xr_dataset_forecast = xr_dataset_forecast.rio.write_crs("epsg:4326")

            # xr_dataset_forecast = xr_dataset_forecast.rio.clip_box(*self.malawi_bbox)

            # vars_to_drop = [
            #     x
            #     for x in xr_dataset_forecast.variables
            #     if x not in ["time", "lat", "lon", "apcpsfc"]
            # ]

            # xr_dataset_hindcast = xr_dataset_hindcast.drop_vars(vars_to_drop)
            # xr_dataset_forecast = xr_dataset_forecast.drop_vars(vars_to_drop)

            # xr_dataset_hindcast.to_netcdf(
            #     r"d:\VSCode\IBF-flash-flood-pipeline\data\GFS\xr_hindcast.nc"
            # )
            # xr_dataset_forecast.to_netcdf(
            #     r"d:\VSCode\IBF-flash-flood-pipeline\data\GFS\xr_forecast.nc"
            # )

            latvar_hind, lonvar_hind = extract_lat_lon(ds=nc_dataset_hindcast)
            latvar_fc, lonvar_fc = extract_lat_lon(ds=nc_dataset_forecast)

            ta_gdf_4326 = self.ta_gdf.copy()
            ta_gdf_4326.to_crs(4326, inplace=True)

            ta_gdf_4326["centr"] = ta_gdf_4326.centroid

            time_hindcast = [
                datetime(year=1, month=1, day=1) + timedelta(days=d)
                for d in nc_dataset_hindcast["time"][:]
            ]

            time_forecast = [
                datetime(year=1, month=1, day=1) + timedelta(days=d)
                for d in nc_dataset_forecast["time"][:]
            ]

            for _, row in ta_gdf_4326.iterrows():
                iy_min_hist, ix_min_hist = tunnel_fast(
                    latvar=latvar_hind,
                    lonvar=lonvar_hind,
                    lat0=row.centr.x,
                    lon0=row.centr.y,
                )
                iy_min_fc, ix_min_fc = tunnel_fast(
                    latvar=latvar_fc,
                    lonvar=lonvar_fc,
                    lat0=row.centr.x,
                    lon0=row.centr.y,
                )

                gfs_hindcast_and_forecast = []

                for ix_min, iy_min, time in [
                    (ix_min_hist, iy_min_hist, time_hindcast),
                    (ix_min_fc, iy_min_fc, time_forecast),
                ]:
                    gfs_rain_ts_cumulative = [
                        float(x) if x != "--" else 0.0
                        for x in nc_dataset_hindcast["apcpsfc"][:, ix_min, iy_min]
                    ]
                    gfs_rain_ts_incremental = [
                        val - gfs_rain_ts_cumulative[i - 1] if i > 0 else val
                        for i, val in enumerate(gfs_rain_ts_cumulative)
                    ]
                    gfs_hindcast_and_forecast.append(
                        pd.DataFrame(
                            data={
                                "datetime": time,
                                "precipitation": gfs_rain_ts_incremental,
                            }
                        )
                    )

                gfs_precipitation = pd.concat(gfs_hindcast_and_forecast, axis=0)
                gfs_precipitation = gfs_precipitation.sort_values("datetime")
                gfs_precipitation = gfs_precipitation.drop_duplicates(
                    subset="datetime", keep="last"
                )
                gfs_data[row["placeCode"]] = gfs_precipitation

            # combined_rainfall = []
            # for _, val in gfs_data.items():
            #     val = val.set_index("datetime")
            #     combined_rainfall.append(val)

            # pd.concat(combined_rainfall, axis=1).to_csv(
            #     r"d:\VSCode\IBF-flash-flood-pipeline\data\gfs_rainfall_prediction.csv"
            # )

        else:
            logger.info("Retrieving COSMO-data")
            ta_gdf_4326 = self.ta_gdf.copy()
            ta_gdf_4326.to_crs(4326, inplace=True)

            logger.info("open_rasterio COSMO-data")
            xds = rioxarray.open_rasterio(expected_cosmo_forecast_path)
            
            logger.info("write_crs COSMO-data")
            xds.rio.write_crs("epsg:4326", inplace=True)

            upscale_factor = 8
            logger.info("Upscaling/reprojecting COSMO-data")
            new_width = xds.rio.width * upscale_factor
            new_height = xds.rio.height * upscale_factor
            xds_upsampled_forecast = xds.rio.reproject(
                xds.rio.crs,
                shape=(new_height, new_width),
                resampling=Resampling.bilinear,
            )
            logger.info("Done")
            datetime_list_forecast = [
                datetime.strptime(x.isoformat(), "%Y-%m-%dT%H:%M:%S")
                for x in xds_upsampled_forecast.time.data[:]
            ]

            past = now - timedelta(days=5)
            xds = rioxarray.open_rasterio(expected_cosmo_hindcast_path)
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

                gfs_data[row["placeCode"]] = pd.concat(
                    [
                        gfs_data[row["placeCode"]],
                        pd.DataFrame(
                            {
                                "datetime": datetime_list_forecast,
                                "precipitation": mean_rain_ts,
                            }
                        ),
                    ],
                    axis=0,
                )
                gfs_data[row["placeCode"]] = gfs_data[row["placeCode"]].drop_duplicates(
                    subset="datetime", keep="last"
                )
                gfs_data[row["placeCode"]] = gfs_data[row["placeCode"]].sort_values(
                    "datetime", ascending=True
                )

        return gfs_data
