import sys

sys.path.append(r"D:\VSCode\IBF-flash-flood-pipeline\flash_flood_pipeline")

from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import netCDF4 as nc
from pathlib import Path
import xarray as xr
import rioxarray
import xvec
from rasterio.enums import Resampling
from utils.general_utils.round_to_nearest_hour import (
    round_to_nearest_hour,
)
import logging
import geopandas as gpd

logger = logging.getLogger(__name__)


def convert_to_xr(ds, bbox=None, parameter_to_obtain="apcpsfc"):
    xr_dataset = xr.open_dataset(xr.backends.NetCDF4DataStore(ds))

    xr_dataset = xr_dataset.rio.set_spatial_dims("lat", "lon")
    xr_dataset = xr_dataset.rio.write_crs("epsg:4326")
    
    vars_to_drop = [
        x
        for x in xr_dataset.variables
        if x not in ["time", "lat", "lon", "spatial_ref", parameter_to_obtain]
    ]

    xr_dataset = xr_dataset.drop_vars(vars_to_drop)
    xr_dataset = xr_dataset.rename({"lat": "y", "lon": "x"})

    if bbox:
        xr_dataset = xr_dataset.rio.clip_box(*bbox)

    return xr_dataset


class GfsDownload:
    def __init__(self, ta_gdf, date):
        self.malawi_bbox = (
            31.0000000000000000,  # lon min
            -19.0000000000000000,  # lat min
            38.0000000000000000,  # lon max
            -7.0000000000000000,  # lat max
        )
        self.ta_shapes = ta_gdf
        self.date = date
        self.gfs_parameter_to_obtain = "apcpsfc"

    @property
    def forecast_start(self):
        forecast_start = round_to_nearest_hour(self.date) - timedelta(hours=9)
        if round(forecast_start.hour / 6) * 6 == 24:
            forecast_start = forecast_start + timedelta(days=1)
        return forecast_start

    @property
    def forecast_start_hour(self):
        forecast_start_hour = round(self.forecast_start.hour / 6) * 6

        if forecast_start_hour < 12:
            forecast_start_hour = "0" + str(forecast_start_hour)

        if forecast_start_hour == 24:
            forecast_start_hour = "00"

        return forecast_start_hour

    def retrieve(self):
        logger.info("GfsDownload - Retrieving GFS-precipitation data")
        url = "https://nomads.ncep.noaa.gov/dods/gfs_0p25/gfs{}/gfs_0p25_{}z".format(
            self.forecast_start.strftime("%Y%m%d"), self.forecast_start_hour
        )

        nc_dataset_forecast = nc.Dataset(
            "https://nomads.ncep.noaa.gov/dods/gfs_0p25/gfs{}/gfs_0p25_{}z".format(
                self.forecast_start.strftime("%Y%m%d"), self.forecast_start_hour
            )
        )
        # surface total precipitation [kg/m^2]

        xr_dataset = convert_to_xr(
            ds=nc_dataset_forecast,
            bbox=self.malawi_bbox,
            parameter_to_obtain=self.gfs_parameter_to_obtain,
        )
   
        upscale_factor = 8

        new_width = xr_dataset.rio.width * upscale_factor
        new_height = xr_dataset.rio.height * upscale_factor
        xr_dataset_upsampled = xr_dataset.rio.reproject(
            xr_dataset.rio.crs,
            shape=(new_height, new_width),
            resampling=Resampling.bilinear,
        )

        return xr_dataset_upsampled

    def sample(self, dataset):
        ta_shapes_4326 = self.ta_shapes.to_crs("epsg:4326")

        sampled = dataset.xvec.zonal_stats(
            ta_shapes_4326.geometry,
            x_coords="x",
            y_coords="y",
            stats=np.nanmean,
            all_touched=False,
        )
        gfs_rainfall = sampled.xvec.to_geodataframe().reset_index(drop=False)

        gfs_rainfall["ta"] = gfs_rainfall.apply(
            lambda row: ta_shapes_4326.loc[
                ta_shapes_4326.geometry == row.geometry, "placeCode"
            ].iloc[0],
            axis=1,
        )
        gfs_rainfall_pvt = gfs_rainfall.pivot(
            index="time", columns="ta", values=self.gfs_parameter_to_obtain
        )
        gfs_rainfall_pvt = gfs_rainfall_pvt.diff()
        gfs_rainfall_pvt = gfs_rainfall_pvt.fillna(0)
        gfs_rainfall_pvt_resampled = (
            gfs_rainfall_pvt.resample("h").bfill().divide(3)
        )  # from mm/3h to mm/h
        return gfs_rainfall_pvt_resampled


if __name__ == "__main__":
    ta_gdf = gpd.read_file(
        r"d:\VSCode\IBF-flash-flood-pipeline\data\static_data\prod\regions.gpkg"
    )
    gfs = GfsDownload(ta_gdf=ta_gdf)

    xr_hindcast, xr_forecast = gfs.retrieve()

    rainfall_timeseries = gfs.sample(dataset=xr_hindcast)
    print(rainfall_timeseries)
# def download_gfs():
#     logger.info("Downloading GFS-precipitation data")
#     hindcast_start = round_to_nearest_hour(datetime.now()) - timedelta(days=2, hours=3)

#     forecast_start = round_to_nearest_hour(datetime.now()) - timedelta(hours=9)

#     forecast_start_hour = round(forecast_start.hour / 6) * 6

#     if forecast_start_hour < 12:
#         forecast_start_hour = "0" + str(forecast_start_hour)

#     if forecast_start_hour == 24:
#         forecast_start_hour = "00"
#         forecast_start = forecast_start + timedelta(days=1)

#     xr_dataset_hindcast = xr.open_dataset(
#         xr.backends.NetCDF4DataStore(nc_dataset_hindcast)
#     )
#     xr_dataset_hindcast = xr_dataset_hindcast.rio.set_spatial_dims("lon", "lat")
#     xr_dataset_hindcast = xr_dataset_hindcast.rio.write_crs("epsg:4326")

#     xr_dataset_hindcast = xr_dataset_hindcast.rio.clip_box(*self.malawi_bbox)

#     xr_dataset_forecast = xr.open_dataset(
#         xr.backends.NetCDF4DataStore(nc_dataset_forecast)
#     )
#     xr_dataset_forecast = xr_dataset_forecast.rio.set_spatial_dims("lon", "lat")
#     xr_dataset_forecast = xr_dataset_forecast.rio.write_crs("epsg:4326")

#     xr_dataset_forecast = xr_dataset_forecast.rio.clip_box(*self.malawi_bbox)

#     vars_to_drop = [
#         x
#         for x in xr_dataset_forecast.variables
#         if x not in ["time", "lat", "lon", "apcpsfc"]
#     ]

#     xr_dataset_hindcast = xr_dataset_hindcast.drop_vars(vars_to_drop)
#     xr_dataset_forecast = xr_dataset_forecast.drop_vars(vars_to_drop)

#     xr_dataset_hindcast.to_netcdf(
#         r"d:\VSCode\IBF-flash-flood-pipeline\data\GFS\xr_hindcast.nc"
#     )
#     xr_dataset_forecast.to_netcdf(
#         r"d:\VSCode\IBF-flash-flood-pipeline\data\GFS\xr_forecast.nc"
#     )

#     latvar_hind, lonvaxr_hind = extract_lat_lon(ds=nc_dataset_hindcast)
#     latvar_fc, lonvar_fc = extract_lat_lon(ds=nc_dataset_forecast)

#     ta_gdf_4326 = self.ta_gdf.copy()
#     ta_gdf_4326.to_crs(4326, inplace=True)

#     ta_gdf_4326["centr"] = ta_gdf_4326.centroid

#     time_hindcast = [
#         datetime(year=1, month=1, day=1) + timedelta(days=d)
#         for d in nc_dataset_hindcast["time"][:]
#     ]

#     time_forecast = [
#         datetime(year=1, month=1, day=1) + timedelta(days=d)
#         for d in nc_dataset_forecast["time"][:]
#     ]

#     for _, row in ta_gdf_4326.iterrows():
#         iy_min_hist, ix_min_hist = tunnel_fast(
#             latvar=latvar_hind,
#             lonvar=lonvar_hind,
#             lat0=row.centr.x,
#             lon0=row.centr.y,
#         )
#         iy_min_fc, ix_min_fc = tunnel_fast(
#             latvar=latvar_fc,
#             lonvar=lonvar_fc,
#             lat0=row.centr.x,
#             lon0=row.centr.y,
#         )

#         gfs_hindcast_and_forecast = []

#         for ix_min, iy_min, time in [
#             (ix_min_hist, iy_min_hist, time_hindcast),
#             (ix_min_fc, iy_min_fc, time_forecast),
#         ]:
#             gfs_rain_ts_cumulative = [
#                 float(x) if x != "--" else 0.0
#                 for x in nc_dataset_hindcast["apcpsfc"][:, ix_min, iy_min]
#             ]
#             gfs_rain_ts_incremental = [
#                 val - gfs_rain_ts_cumulative[i - 1] if i > 0 else val
#                 for i, val in enumerate(gfs_rain_ts_cumulative)
#             ]
#             gfs_hindcast_and_forecast.append(
#                 pd.DataFrame(
#                     data={
#                         "datetime": time,
#                         "precipitation": gfs_rain_ts_incremental,
#                     }
#                 )
#             )

#         gfs_precipitation = pd.concat(gfs_hindcast_and_forecast, axis=0)
#         gfs_precipitation = gfs_precipitation.sort_values("datetime")
#         gfs_precipitation = gfs_precipitation.drop_duplicates(
#             subset="datetime", keep="last"
#         )
#         gfs_data[row["placeCode"]] = gfs_precipitation
