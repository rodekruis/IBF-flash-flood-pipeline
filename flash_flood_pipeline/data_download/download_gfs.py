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
import warnings

warnings.filterwarnings("ignore")


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
        gfs_rainfall = gfs_rainfall.reset_index(drop=True)
        gfs_rainfall_pvt = gfs_rainfall.pivot(
            index="time", columns="ta", values=self.gfs_parameter_to_obtain
        )
        gfs_rainfall_pvt = gfs_rainfall_pvt.diff()
        gfs_rainfall_pvt = gfs_rainfall_pvt.fillna(0)
        gfs_rainfall_pvt_resampled = (
            gfs_rainfall_pvt.resample("h").bfill().divide(3)
        )  # from mm/3h to mm/h
        return gfs_rainfall_pvt_resampled