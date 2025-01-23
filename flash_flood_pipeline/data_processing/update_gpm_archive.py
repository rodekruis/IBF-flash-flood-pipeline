from pathlib import Path
import sys

sys.path.append(r"D:\VSCode\IBF-flash-flood-pipeline\flash_flood_pipeline")
from data_download.download_gpm import GpmDownload
import xarray as xr

import rioxarray
import numpy as np
import logging
import geopandas as gpd
from rasterio.enums import Resampling
import xvec
logger = logging.getLogger(__name__)


def update_rain_archive(ta_gdf):
    download_path = Path(r"data\gpm\raw")
    gpm_download = GpmDownload(download_path=download_path)

    gpm_download.get_catalogs()
    urls = gpm_download.get_urls()

    gpm_download.download_hdf(urls=urls)

    is_valid, nc_start_date, nc_end_date = gpm_download.validate_hdf()
    logger.info(
        f"GPM archive up to date from {nc_start_date} to {nc_end_date}. No temporal datagaps: {is_valid}"
    )
    xr_output_path = gpm_download.process_data()

    ta_shapes_4326 = ta_gdf.to_crs("epsg:4326")

    dataset = rioxarray.open_rasterio(xr_output_path)

    upscale_factor = 8

    new_width = dataset.rio.width * upscale_factor
    new_height = dataset.rio.height * upscale_factor
    dataset_upsampled = dataset.rio.reproject(
        dataset.rio.crs,
        shape=(new_height, new_width),
        resampling=Resampling.bilinear,
    )

    sampled = dataset_upsampled.xvec.zonal_stats(
        ta_shapes_4326.geometry,
        x_coords="x",
        y_coords="y",
        stats=np.nanmean,
        all_touched=False,
    )

    gpm_rainfall = sampled.xvec.to_geodataframe().reset_index(drop=False)

    gpm_rainfall["ta"] = gpm_rainfall.apply(
        lambda row: ta_shapes_4326.loc[
            ta_shapes_4326.geometry == row.geometry, "placeCode"
        ].iloc[0],
        axis=1,
    )
    
    gpm_rainfall = gpm_rainfall.pivot(
        index="time", columns="ta", values="gpm_precipitation"
    )
    return gpm_rainfall
