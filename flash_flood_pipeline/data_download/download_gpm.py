import os
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, time
import requests
import rasterio
import h5py
import pandas as pd
import xarray as xr
from pathlib import Path
import rioxarray
from concurrent.futures import ThreadPoolExecutor
import logging
from .ftp import download_file, get_remote_filepaths_in_daterange

logger = logging.getLogger(__name__)

FTP_HOST = "jsimpsonftps.pps.eosdis.nasa.gov"
IMERG_LATE_FTP_DIR = "data/imerg/late"


class GpmDownload:
    def __init__(
        self,
        download_path: Path,
        t0: datetime = datetime.now(),
        ensure_available_days: int = 7,
    ):
        self.ftp_user = os.getenv("FTP_USERNAME")

        if self.ftp_user == None:
            raise ValueError(
                "Set a valid FTP_USERNAME for ftp-server jsimpsonftps.pps.eosdis.nasa.gov as env variable"
            )
        
        self.missing_days = []
        self.download_path = download_path

        self.malawi_bounds = (
            31.0000000000000000,  # lon min
            -19.0000000000000000,  # lat min
            38.0000000000000000,  # lon max
            -7.0000000000000000,  # lat max
        )
        self.t0 = t0
        self.archive_start_date = self.t0 - timedelta(days=ensure_available_days)

    def get_urls(self):
        ftp_urls = get_remote_filepaths_in_daterange(
            username=self.ftp_user, start_date=self.archive_start_date, end_date=self.t0
        )
        return ftp_urls


    def gpm_request(self, download_meta: tuple):
        """download_meta: (url, filename)"""
        raw = download_file(host=FTP_HOST, username=self.ftp_user, remote_filepath=download_meta[1])

        try:
            with open(self.download_path / download_meta[0], "wb") as f:
                f.write(raw[1].getbuffer())
                return (True, download_meta[1])
        except Exception as e:
            logger.error(f"Failed to save GPM file {download_meta[0]}: {e}")
            return (False, download_meta[1])

    def download_hdf(self, urls):
        self.download_path.mkdir(parents=True, exist_ok=True)
        self.filenames = []

        download_meta_tuples = []

        for url in urls:
            filename = os.path.split(url)[1]
            if not (self.download_path / filename).exists():
                download_meta_tuples.append((filename, url))

        with ThreadPoolExecutor(max_workers=5) as executor:
            output_paths = executor.map(self.gpm_request, download_meta_tuples)

        downloaded_paths = []
        failed_paths = []

        for success, path in output_paths:
            if success:
                downloaded_paths.append(path)
            else:
                failed_paths.append(path)

    def validate_hdf(self):
        start_date = datetime.combine(
            self.archive_start_date.date(), time(hour=0, minute=0, second=0)
        )

        # clean
        hdf5_paths = [
            p for p in self.download_path.glob("*")
            if p.is_file() and p.suffix in (".HDF5", ".RT-H5")
        ]
        hdf5_dates = [
            datetime.strptime(
                f'{p.stem.split("IMERG.")[-1].split("-S")[0]} {p.stem.split("-S")[-1].split("-")[0]}',
                "%Y%m%d %H%M%S",
            )
            for p in hdf5_paths
        ]

        hdf5_index = {k: v for k, v in zip(hdf5_paths, hdf5_dates)}

        for path, date in hdf5_index.items():
            if date < start_date:
                path.unlink()

        # reload
        hdf5_paths = [p for p in hdf5_paths if p.exists()]
        self.filenames = [p.name for p in hdf5_paths]
        hdf5_dates = [
            datetime.strptime(
                f'{p.stem.split("IMERG.")[-1].split("-S")[0]} {p.stem.split("-S")[-1].split("-")[0]}',
                "%Y%m%d %H%M%S",
            )
            for p in hdf5_paths
        ]

        if not hdf5_dates:
            raise RuntimeError(
                f"No GPM HDF5 files (.HDF5 or .RT-H5) on or after {start_date} remain in "
                f"{self.download_path.resolve()}. Check FTP file availability, "
                "download errors, and write permissions on the mounted data directory."
            )

        hdf5_dates.sort()
        expected_daterange = pd.date_range(
            start=start_date, end=hdf5_dates[-1], freq="30min"
        )

        no_gap_bool = all([x in hdf5_dates for x in expected_daterange])

        with open(self.download_path.parent / "gpm_meta.txt", "w") as dst:
            dst.writelines(
                [
                    "GPM Quality Report\n",
                    f"Start date: {start_date}\n",
                    f"End date: {hdf5_dates[-1]}\n",
                    f"No gaps: {no_gap_bool}",
                ]
            )

        return no_gap_bool, hdf5_dates[0], hdf5_dates[-1]

    def process_data(self):
        lonbounds = (self.malawi_bounds[0], self.malawi_bounds[2])
        latbounds = (self.malawi_bounds[1], self.malawi_bounds[3])

        self.timestamps = []

        xr_datasets = []

        for filename in self.filenames:
            try:
                dataset = h5py.File(os.path.join(self.download_path, filename), "r")

                lats = dataset["Grid"]["lat"][:]
                lons = dataset["Grid"]["lon"][:]

                timestamp = datetime.strptime(
                    str(dataset["Grid"]["time"].attrs["Units"]),
                    "b'seconds since %Y-%m-%d %H:%M:%S UTC'",
                ) + timedelta(seconds=int(dataset["Grid"]["time"][0]))

                precipitation_all = dataset["Grid"]["precipitation"][0, :, :]
                lat_index = [
                    index
                    for index, value in enumerate(lats)
                    if (value > latbounds[0]) & (value < latbounds[1])
                ]

                lon_index = [
                    index
                    for index, value in enumerate(lons)
                    if (value > lonbounds[0]) & (value < lonbounds[1])
                ]

                lat_index = [lat_index[0], lat_index[-1]]
                lon_index = [lon_index[0], lon_index[-1]]

                precip = precipitation_all[
                    lon_index[0] : lon_index[1] + 1, lat_index[0] : lat_index[1] + 1
                ]

                rev = range(len(precip[0, :]) - 1, -1, -1)
                precip = precip.transpose()[rev, :]

                with rasterio.Env(GDAL_PAM_ENABLED=False):
                    with rasterio.io.MemoryFile() as memfile:
                        with memfile.open(
                            driver="GTiff",
                            width=(lonbounds[1] - lonbounds[0]) * 10,
                            height=(latbounds[1] - latbounds[0]) * 10,
                            count=1,
                            dtype=precip.dtype,
                            crs="EPSG:4326",
                            transform=rasterio.transform.from_origin(
                                lonbounds[0], latbounds[1], 0.1, 0.1
                            ),
                            nodata=-1,
                        ) as dst:
                            dst.write(precip, 1)

                        xr_datasets.append(rioxarray.open_rasterio(memfile).load())
                    self.timestamps.append(timestamp)
            except Exception as e:
                logger.warning(f"Skipping {filename} due to Exception: {e}")

        time = xr.Variable("time", self.timestamps)

        da = xr.concat([f for f in xr_datasets], dim=time).rename("gpm_precipitation")
        da = da.rio.write_crs("epsg:4326")
        da = da.rio.set_spatial_dims("x", "y")
        da = da.isel(band=0)

        da = da.drop("band")

        output_path = Path(r"data/gpm/gpm_rolling_week.nc")
        da.to_netcdf(output_path)
        return output_path
