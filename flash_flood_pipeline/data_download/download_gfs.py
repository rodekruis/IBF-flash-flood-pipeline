import sys

sys.path.append(r"D:\VSCode\IBF-flash-flood-pipeline\flash_flood_pipeline")

from datetime import datetime, timedelta
import time
import requests
import numpy as np
import pandas as pd
import netCDF4 as nc
from pathlib import Path
import xarray as xr
import rioxarray
import xvec
from uuid import uuid4
import cfgrib
from rasterio.enums import Resampling
from utils.general_utils.round_to_nearest_hour import (
    round_to_nearest_hour,
)
import logging
import geopandas as gpd
import warnings

warnings.filterwarnings("ignore")


logger = logging.getLogger(__name__)


CFGRIB_BACKEND_KWARGS = {
    "indexpath": "",
    "filter_by_keys": {"stepType": "accum"},
}


def validate_grib_file(
    filepath: str,
    min_file_size: int = 1000,
    require_cfgrib_open: bool = False,
    backend_kwargs: dict | None = None,
) -> bool:
    """
    Validate that a downloaded file is a valid GRIB file.
    
    Args:
        filepath: Path to the GRIB file to validate
        min_file_size: Minimum acceptable file size in bytes
        require_cfgrib_open: Whether the file must also be readable by xarray/cfgrib
        backend_kwargs: Optional cfgrib backend kwargs used when require_cfgrib_open is True
    
    Returns:
        True if file is valid GRIB, False otherwise
    """
    file_path = Path(filepath)
    
    # Check if file exists and has minimum size
    if not file_path.exists():
        logger.warning(f"File does not exist: {filepath}")
        return False
    
    if file_path.stat().st_size < min_file_size:
        logger.warning(f"File too small ({file_path.stat().st_size} bytes): {filepath}")
        return False
    
    # First do a lightweight GRIB header sanity check.
    try:
        with open(filepath, "rb") as f:
            cfgrib.messages.FileStream(f)
    except Exception as e:
        logger.warning(f"GRIB validation failed for {filepath}: {e}")
        return False

    if not require_cfgrib_open:
        logger.debug(f"GRIB header validation passed for: {filepath}")
        return True

    # Use the same cfgrib settings as the final multi-file load to catch incompatible files early.
    try:
        test_ds = xr.open_dataset(
            filepath,
            engine="cfgrib",
            backend_kwargs=backend_kwargs,
        )
        test_ds.close()
        logger.debug(f"GRIB open validation passed for: {filepath}")
        return True
    except Exception as e:
        logger.warning(f"GRIB open validation failed for {filepath}: {e}")
        return False


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

def formulate_gfs_urls(
        bbox: tuple[float, float, float, float], 
        forecast_start: datetime, 
        forecast_start_hour: str
    ) -> list[str]:
    west, south, east, north = bbox
    forecast_start = forecast_start.strftime("%Y%m%d")
    var_ACPCP = "on"
    lev_surface = "on"

    base_url = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"
    dir = f"%2Fgfs.{forecast_start}%2F{forecast_start_hour}%2Fatmos"
    subregion = f"&toplat={north}&leftlon={west}&rightlon={east}&bottomlat={south}"
    
    urls = []
    time_step_end = 49
    for h in range(0, time_step_end, 1):  # from 0 to time_step_end, every 1 hour
        file = f"gfs.t{forecast_start_hour}z.pgrb2.0p25.f{h:03d}"
        url = fr"{base_url}?dir={dir}&file={file}&var_ACPCP={var_ACPCP}&lev_surface={lev_surface}&subregion={subregion}"
        urls.append(url)

    return urls

def download_url_with_retries(
        url: str, 
        max_attempts: int = 5, 
        retry_interval: int = 5) -> str | None:

    attempt = 1
    while attempt <= max_attempts:
        try:
            r = requests.get(url)
            r.raise_for_status()
            local_file = f"gfs_{attempt}_{int(time.time())}_{uuid4().hex[:8]}.grb2"
            with open(local_file, "wb") as f:
                f.write(r.content)
            
            # Validate GRIB file before returning
            if validate_grib_file(local_file):
                logger.info(f"Successfully downloaded and validated: {local_file}")
                return local_file
            else:
                # File is invalid, delete it and retry
                Path(local_file).unlink(missing_ok=True)
                logger.warning(f"Downloaded file failed validation, retrying...")
                if attempt < max_attempts:
                    time.sleep(retry_interval)
                attempt += 1
                continue
                
        except Exception as e:
            logger.warning(f"Attempt {attempt}: Failed to retrieve data from {url}: {e}")
            if attempt < max_attempts:
                time.sleep(retry_interval)
            attempt += 1
    logger.warning(f"All retries failed for {url}. Moving to next URL.")
    return None

def request_gfs_data(urls: list[str]) -> list[str]:

    failed_urls = []
    successful_files = []
    for url in urls:
        local_file = download_url_with_retries(url)
        if local_file:
            successful_files.append(local_file)
        else:
            failed_urls.append(url)
    if failed_urls:
        sorted_failed_url = sorted(failed_urls)
        logger.warning(f"Failed URLs: {sorted_failed_url}")
    if successful_files:
        logger.info(f"Successfully downloaded files: {successful_files}")
    return sorted(successful_files)

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

        urls = formulate_gfs_urls(
            bbox=self.malawi_bbox,
            forecast_start=self.forecast_start,
            forecast_start_hour=self.forecast_start_hour
        )
        downloaded_files = request_gfs_data(urls)
        
        logger.info(
            f"Downloaded {len(downloaded_files)} files. Validating each file with cfgrib before combining..."
        )

        valid_files = []
        for file in downloaded_files:
            if validate_grib_file(
                file,
                require_cfgrib_open=True,
                backend_kwargs=CFGRIB_BACKEND_KWARGS,
            ):
                valid_files.append(file)
            else:
                Path(file).unlink(missing_ok=True)
        
        if not valid_files:
            raise ValueError(
                f"No valid GFS GRIB files could be opened. "
                f"Downloaded {len(downloaded_files)} files but all failed validation."
            )
        
        logger.info(f"Successfully validated {len(valid_files)} out of {len(downloaded_files)} downloaded files")
        
        logger.info(
            "Loading GFS files with xarray.open_mfdataset using combine='nested' and cfgrib accum-step filtering"
        )
        xr_dataset = xr.open_mfdataset(
            valid_files,
            combine="nested",
            engine="cfgrib",
            backend_kwargs=CFGRIB_BACKEND_KWARGS,
            coords="minimal",
            compat="override",
            chunks="auto",
        )

        # Set CRS for GFS data (WGS84)
        xr_dataset = xr_dataset.rio.write_crs("EPSG:4326")

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