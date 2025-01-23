from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from pathlib import Path
import xarray as xr
import rioxarray
from rasterio.enums import Resampling
import logging

logger = logging.getLogger(__name__)


def process_cosmo(ta_gdf, cosmo_path: Path):
    logger.info("Processing COSMO-data")

    ta_gdf_4326 = ta_gdf.to_crs(4326)

    cosmo_data = {}

    xds = rioxarray.open_rasterio(cosmo_path)
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

    for _, row in ta_gdf_4326.iterrows():
        xds_clipped = xds_upsampled_forecast.rio.clip(
            [row["geometry"]], ta_gdf_4326.crs
        )
        xds_clipped.data[xds_clipped.data > 1000] = np.nan
        cum_mean_rain_ts = [np.nanmean(x) for x in xds_clipped.data]
        mean_rain_ts = [cum_mean_rain_ts[0]]

        for x in range(1, len(cum_mean_rain_ts)):
            mean_rain_ts.append(cum_mean_rain_ts[x] - cum_mean_rain_ts[x - 1])

        cosmo_data[row["placeCode"]] = pd.DataFrame(
            {
                "datetime": datetime_list_forecast,
                "precipitation": mean_rain_ts,
            }
        )
        cosmo_data[row["placeCode"]] = cosmo_data[row["placeCode"]].sort_values(
            "datetime", ascending=True
        )
    
    individual_timeseries = []
    
    for col_name, timeseries in cosmo_data.items():
        values_renamed = timeseries.rename(columns={"precipitation": col_name})
        values_renamed = values_renamed.set_index("datetime")
        individual_timeseries.append(values_renamed)
        
    cosmo_df = pd.concat(individual_timeseries, axis=1)
    return cosmo_df
