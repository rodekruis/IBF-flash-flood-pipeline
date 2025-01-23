import geopandas as gpd
from pathlib import Path
import datetime
from data_processing.process_cosmo import process_cosmo
from data_download.download_gfs import GfsDownload
from data_download.download_gpm import GpmDownload
import logging
import rioxarray
from rasterio.enums import Resampling
import xvec
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class CosmoNotFound(Exception):
    pass


class ForcingProcessor:
    def __init__(self, ta_gdf: gpd.GeoDataFrame):
        self.ta_gdf = ta_gdf
        self.current_date_utc = (
            datetime.datetime.now()
            .astimezone(datetime.timezone.utc)
            .replace(tzinfo=None)
        )
        print(self.current_date_utc)
        self.cosmo_folder = Path("data/cosmo")

    @property
    def most_recent_cosmo_date(self):
        latest_cosmo_date = max(
            [
                datetime.datetime.strptime(
                    date_string.name, "COSMO_MLW_%Y%m%dT%H_prec.nc"
                )
                for date_string in self.cosmo_folder.glob("*.nc")
            ]
        )
        return latest_cosmo_date

    @property
    def cosmo_prediction_found(self):
        """
        Check if eligible COSMO data is available.

        Criteria:
        - COSMO from same day OR
        - COSMO from previous day if COSMO from same day is not available AND run starts before 07:00 AM UTC
        """
        if self.most_recent_cosmo_date.date() == self.current_date_utc.date():
            return True
        elif (
            self.most_recent_cosmo_date.date()
            == self.current_date_utc.date() - datetime.timedelta(days=1)
        ):
            if self.current_date_utc < self.current_date_utc.replace(
                hour=7, minute=0, second=0
            ):
                return True
            else:
                return False
        else:
            return False

    @property
    def cosmo_date_to_use(self):
        """
        COSMO is available around 06:00 AM UTC. If a run kicks of before that, use COSMO from the previous day
        """
        if self.most_recent_cosmo_date.date() == self.current_date_utc.date():
            return self.current_date_utc.replace(
                hour=0, minute=0, second=0, microsecond=0
            )
        elif (
            self.most_recent_cosmo_date.date()
            == self.current_date_utc.date() - datetime.timedelta(days=1)
        ):
            if self.current_date_utc < self.current_date_utc.replace(
                hour=7, minute=0, second=0
            ):
                return (self.current_date_utc - datetime.timedelta(days=1)).replace(
                    hour=0, minute=0, second=0, microsecond=0
                )
            else:
                raise CosmoNotFound("No eligible COSMO-data found")
        else:
            raise CosmoNotFound("No eligible COSMO-data found")

    def update_gpm_archive(self):
        download_path = Path(r"data\forcing\gpm\raw")
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
        gpm_rainfall.index = [pd.to_datetime(str(date)) for date in gpm_rainfall.index]
        gpm_rainfall = gpm_rainfall.sort_index()
        gpm_rainfall = gpm_rainfall.resample("h").mean()
        gpm_rainfall["src"] = "GPM"
        return gpm_rainfall

    def retrieve_forecast(self):
        if self.cosmo_prediction_found:
            logger.info("Eligible COSMO-data found.")

            cosmo_path = Path(
                r"data/cosmo/COSMO_MLW_{}T00_prec.nc".format(
                    self.cosmo_date_to_use.strftime("%Y%m%d")
                )
            )
            forcing_forecast = process_cosmo(ta_gdf=ta_gdf, cosmo_path=cosmo_path)
            forcing_forecast["src"] = "COSMO"
        else:
            logger.info("Eligible COSMO-data not found, switching to GFS.")

            gfs_data = GfsDownload(ta_gdf=ta_gdf, date=self.current_date_utc)
            xr_gfs_forecast = gfs_data.retrieve()
            forcing_forecast = gfs_data.sample(
                dataset=xr_gfs_forecast
            )  # TODO: Unit conversion
            forcing_forecast["src"] = "GFS"
        return forcing_forecast

    def construct_forcing_timeseries(self):
        print("updating archive")
        #gpm_archive_df = self.update_gpm_archive()
        # gpm_archive_df.to_csv(r"d:\Documents\3_Projects\Training Ghana\HEC-RAS model\example_model\2023_dredged\gpm_archive.csv")

        gpm_archive_df = pd.read_csv(
            r"d:\Documents\3_Projects\Training Ghana\HEC-RAS model\example_model\2023_dredged\gpm_archive.csv",
            index_col=0,
            parse_dates=True,
        )
        last_gpm_timestep = gpm_archive_df.index[-1]
        gpm_interval = datetime.timedelta(hours=0.5)
        print("retrieving forecast")
        forcing_forecast = self.retrieve_forecast()

        if (
            last_gpm_timestep.to_pydatetime() + gpm_interval
            >= forcing_forecast.index[0].to_pydatetime()
        ):
            print("Prediction fits to GPM")
            forcing_timeseries_datagap = None
        else:
            print("Filling gap between prediction and GPM")
            forcing_gap_start = last_gpm_timestep.to_pydatetime() + gpm_interval
            forcing_gap_end = forcing_forecast.index[0].to_pydatetime()

            cosmo_path_data_gap = Path(
                r"data/cosmo/COSMO_MLW_{}T00_prec.nc".format(
                    forcing_gap_start.strftime("%Y%m%d")
                )
            )    
            if cosmo_path_data_gap.exists():
                logger.info("Filling gap between GPM and prediction with COSMO")
                cosmo_data = process_cosmo(ta_gdf=ta_gdf, cosmo_path=cosmo_path_data_gap)
                forcing_timeseries_datagap = cosmo_data.loc[
                    (cosmo_data.index >= forcing_gap_start)
                    & (cosmo_data.index <= forcing_gap_end)
                ]
                forcing_timeseries_datagap["src"] = "COSMO_GAP"
            else:
                print("GFS")
                print(forcing_gap_start)
                gfs_data = GfsDownload(ta_gdf=ta_gdf, date=forcing_gap_start)
                xr_gfs_forecast = gfs_data.retrieve()
                forcing_timeseries_datagap = gfs_data.sample(dataset=xr_gfs_forecast)
                forcing_timeseries_datagap["src"] = "GFS_GAP"
                forcing_timeseries_datagap = forcing_timeseries_datagap.loc[
                    (forcing_timeseries_datagap.index >= forcing_gap_start)
                    & (forcing_timeseries_datagap.index <= forcing_gap_end)
                ]

            print(gpm_archive_df.head())
            print(gpm_archive_df.tail())
            print("-------"*10)
            try:
                print(forcing_timeseries_datagap.head())
                print(forcing_timeseries_datagap.tail())
            except:
                pass
            print("-------"*10)
            print(forcing_forecast.head())
            print(forcing_forecast.tail())


if __name__ == "__main__":
    ta_gdf = gpd.read_file(
        r"d:\VSCode\IBF-flash-flood-pipeline\data\static_data\prod\regions.gpkg"
    )
    fp = ForcingProcessor(ta_gdf=ta_gdf)
    fp.construct_forcing_timeseries()

    # # else:
    # forcing_gap_start = last_gpm_timestep.to_pydatetime() + gpm_interval
    # forcing_gap_end = datetime.datetime.combine(
    #     fp.cosmo_date_to_use, datetime.time(hour=0, minute=0, second=0)
    # )
    # # try to get cosmo for datagap

    # cosmo_path_data_gap = Path(
    #     r"data/cosmo/COSMO_MLW_{}T00_prec.nc".format(
    #         forcing_gap_start.strftime("%Y%m%d")
    #     )
    # )

    # # if cosmo_path_data_gap.exists():
    # #     logger.info("Filling gap between GPM and prediction with COSMO")
    # #     cosmo_data = process_cosmo(ta_gdf=ta_gdf, cosmo_path=cosmo_path_data_gap)
    # #     print(forcing_gap_start, forcing_gap_end)
    # #     cosmo_data_gap = cosmo_data.loc[
    # #         (cosmo_data.index >= forcing_gap_start)
    # #         & (cosmo_data.index <= forcing_gap_end)
    # #     ]
    # # else:
    # gfs_data = GfsDownload(ta_gdf=ta_gdf, date=forcing_gap_start)
    # xr_gfs_forecast = gfs_data.retrieve()
    # gfs_forecast_timeseries = gfs_data.sample(dataset=xr_gfs_forecast)
    # print(gfs_forecast_timeseries)
