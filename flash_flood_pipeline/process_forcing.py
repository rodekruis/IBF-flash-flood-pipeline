import geopandas as gpd
from pathlib import Path
import datetime
from data_processing.process_cosmo import process_cosmo
from data_processing.process_gpm import update_gpm_archive
from data_download.download_gfs import GfsDownload
import logging
import rioxarray
from rasterio.enums import Resampling
import xvec
import numpy as np
import pandas as pd
from settings.base import ENVIRONMENT


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
        self.cosmo_folder = Path(r"data/cosmo")

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

    def retrieve_forecast(self):
        if self.cosmo_prediction_found:
            logger.info("Eligible COSMO-data found.")

            cosmo_path = Path(
                r"data/cosmo/COSMO_MLW_{}T00_prec.nc".format(
                    self.cosmo_date_to_use.strftime("%Y%m%d")
                )
            )
            forcing_forecast = process_cosmo(ta_gdf=self.ta_gdf, cosmo_path=cosmo_path)
            forcing_forecast["src"] = "COSMO"
        else:
            logger.info("Eligible COSMO-data not found, switching to GFS.")

            gfs_data = GfsDownload(ta_gdf=self.ta_gdf, date=self.current_date_utc)
            xr_gfs_forecast = gfs_data.retrieve()

            xr_gfs_forecast.to_netcdf(
                rf"data\{ENVIRONMENT}\debug_output\gfs_{self.current_date_utc.strftime('%Y%m%d-%H')}.nc"
            )
            forcing_forecast = gfs_data.sample(dataset=xr_gfs_forecast)
            forcing_forecast["src"] = "GFS"
        return forcing_forecast

    def construct_forcing_timeseries(self):
        gpm_archive_df = update_gpm_archive(ta_gdf=self.ta_gdf)

        gpm_archive_df.to_csv(rf"data/{ENVIRONMENT}/debug_output/gpm_archive.csv")

        # gpm_archive_df = pd.read_csv(
        #     r"d:\Documents\3_Projects\Training Ghana\HEC-RAS model\example_model\2023_dredged\gpm_archive.csv",
        #     index_col=0,
        #     parse_dates=True,
        # )
        # gpm_archive_df = gpm_archive_df.drop("src", axis=1).resample("h").mean() # unit is mm/h, timestep is 0.5 h
        # gpm_archive_df["src"] = "GPM"

        last_gpm_timestep = gpm_archive_df.index[-1]

        forcing_forecast = self.retrieve_forecast()

        if (
            last_gpm_timestep.to_pydatetime()
            >= forcing_forecast.index[0].to_pydatetime()
        ):
            forcing_forecast_clipped = forcing_forecast.loc[
                forcing_forecast.index > last_gpm_timestep.to_pydatetime()
            ].copy()
            forcing_combined = pd.concat(
                [gpm_archive_df, forcing_forecast_clipped], axis=0
            )  # drop first row: doublecheck how values are represented
        else:

            forcing_gap_start = last_gpm_timestep.to_pydatetime()
            forcing_gap_end = forcing_forecast.index[0].to_pydatetime()

            cosmo_path_data_gap = Path(
                r"data/cosmo/COSMO_MLW_{}T00_prec.nc".format(
                    forcing_gap_start.strftime("%Y%m%d")
                )
            )

            if cosmo_path_data_gap.exists():
                logger.info("Filling gap between GPM and prediction with COSMO")
                cosmo_data = process_cosmo(
                    ta_gdf=self.ta_gdf, cosmo_path=cosmo_path_data_gap
                )
                forcing_timeseries_datagap = cosmo_data.loc[
                    (cosmo_data.index > forcing_gap_start)
                    & (cosmo_data.index <= forcing_gap_end)
                ].copy()
                forcing_timeseries_datagap["src"] = "COSMO_GAP"
            else:
                logger.info("Filling gap between GPM and prediction with GFS")
                gfs_data = GfsDownload(ta_gdf=self.ta_gdf, date=forcing_gap_start)
                xr_gfs_gap = gfs_data.retrieve()
                forcing_timeseries_datagap = gfs_data.sample(dataset=xr_gfs_gap)
                forcing_timeseries_datagap["src"] = "GFS_GAP"
                forcing_timeseries_datagap = forcing_timeseries_datagap.loc[
                    (forcing_timeseries_datagap.index > forcing_gap_start)
                    & (forcing_timeseries_datagap.index <= forcing_gap_end)
                ]

            forcing_combined = pd.concat(
                [gpm_archive_df, forcing_timeseries_datagap, forcing_forecast[1:]],
                axis=0,
            )  # drop first row: doublecheck how values are represented

        forcing_combined.to_csv(r"data/dev/debug_output/forcing_combined.csv")

        split_forcing_dfs = [
            forcing_combined[[c]]
            .rename(columns={c: "precipitation"})
            .reset_index(names="datetime")
            for c in forcing_combined.columns
            if c != "src"
        ]

        forcing_combined_dict = {
            k: v for k, v in zip(forcing_combined.columns.tolist(), split_forcing_dfs)
        }

        return forcing_combined_dict


if __name__ == "__main__":
    ta_gdf = gpd.read_file(
        r"d:\VSCode\IBF-flash-flood-pipeline\data\static_data\test\regions.gpkg"
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
