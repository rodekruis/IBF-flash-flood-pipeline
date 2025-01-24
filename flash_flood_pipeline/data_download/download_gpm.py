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

logger = logging.getLogger(__name__)


def get_catalog(date):
    catalog_location = f"https://gpm2.gesdisc.eosdis.nasa.gov/opendap/hyrax/GPM_L3/GPM_3IMERGHHL.07/{date.strftime('%Y')}/{date.strftime('%j')}/catalog.xml"
    catalog_raw = requests.get(catalog_location)
    if catalog_raw.status_code == 200:
        catalog = ET.fromstring(catalog_raw.text)
    else:
        catalog = None
    return catalog


class GpmDownload:
    def __init__(
        self,
        download_path: Path,
        t0: datetime = datetime.now(),
        ensure_available_days: int = 7,
    ):
        self.base_url = "https://gpm2.gesdisc.eosdis.nasa.gov/"
        self.missing_days = []
        self.download_path = download_path
        logger.info(f"Download path: {self.download_path} - {self.download_path.exists()}")

        self.malawi_bounds = (
            31.0000000000000000,  # lon min
            -19.0000000000000000,  # lat min
            38.0000000000000000,  # lon max
            -7.0000000000000000,  # lat max
        )
        self.t0 = t0
        self.archive_start_date = self.t0 - timedelta(days=ensure_available_days)

    def get_catalogs(self):
        self.catalogs = {}
        for date in pd.date_range(start=self.archive_start_date, end=self.t0, freq="d"):
            self.catalogs[date.strftime("%Y%m%d")] = get_catalog(date)
            if self.catalogs[date.strftime("%Y%m%d")] == None:
                self.missing_days.append(date.strftime("%Y%m%d"))

        return self.catalogs

    def get_urls(self):
        self.url_dict = {}
        for key, catalog in self.catalogs.items():
            if key not in self.missing_days:
                urls = []
                dataset = catalog[3]
                for child in dataset:
                    if (
                        child.attrib["ID"][
                            len(child.attrib["ID"]) - 5 : len(child.attrib["ID"])
                        ]
                        == ".HDF5"
                    ):
                        url = child.attrib["ID"].replace("opendap/hyrax", "data")
                        urls.append(url)
                self.url_dict[key] = urls
            else:
                self.url_dict[key] = None
        return self.url_dict

    def gpm_request(self, download_meta: tuple):
        """download_meta: (url, filename)"""
        raw = requests.get(self.base_url + download_meta[1])

        if raw.status_code == 200:
            with open(self.download_path / download_meta[0], "wb") as f:
                f.write(raw.content)

            return (True, download_meta[1])
        else:
            return (False, download_meta[1])

    def download_hdf(self, urls):
        self.filenames = []

        download_meta_tuples = []

        for _, date_url_list in urls.items():
            if date_url_list:
                for url in date_url_list:
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
        hdf5_paths = [p for p in self.download_path.glob("*.HDF5")]
        hdf5_dates = [
            datetime.strptime(
                f'{p.stem.split("IMERG.")[-1].split("-S")[0]} {p.stem.split("-S")[-1].split("-")[0]}',
                "%Y%m%d %H%M%S",
            )
            for p in self.download_path.glob("*.HDF5")
        ]

        hdf5_index = {k: v for k, v in zip(hdf5_paths, hdf5_dates)}

        for path, date in hdf5_index.items():
            if date < start_date:
                path.unlink()

        # reload
        hdf5_paths = [p for p in self.download_path.glob("*.HDF5")]
        self.filenames = [p.name for p in self.download_path.glob("*.HDF5")]
        hdf5_dates = [
            datetime.strptime(
                f'{p.stem.split("IMERG.")[-1].split("-S")[0]} {p.stem.split("-S")[-1].split("-")[0]}',
                "%Y%m%d %H%M%S",
            )
            for p in self.download_path.glob("*.HDF5")
        ]

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
            dataset = h5py.File(os.path.join(self.download_path, filename), "r")

            lats = dataset["Grid"]["lat"][:]
            lons = dataset["Grid"]["lon"][:]

            timestamp = datetime.strptime(
                str(dataset["Grid"]["time"].attrs["Units"]),
                "b'seconds since %Y-%m-%d %H:%M:%S UTC'",
            ) + timedelta(seconds=int(dataset["Grid"]["time"][0]))

            self.timestamps.append(timestamp)

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

                    xr_datasets.append(rioxarray.open_rasterio(memfile))

        time = xr.Variable("time", self.timestamps)

        da = xr.concat([f for f in xr_datasets], dim=time).rename("gpm_precipitation")
        da = da.rio.write_crs("epsg:4326")
        da = da.rio.set_spatial_dims("x", "y")
        da = da.drop("band")

        output_path = Path(r"data/gpm/gpm_rolling_week.nc")
        da.to_netcdf(output_path)
        return output_path


if __name__ == "__main__":
    download_path = Path(r"c:\Users\923265\Downloads\GPM\raw")

    datetime_today = datetime.now()

    gpm_download = GpmDownload(download_path=download_path)

    gpm_download.get_catalogs()
    urls = gpm_download.get_urls()
    # print(urls)
    gpm_download.download_hdf(urls=urls)
    is_valid, nc_start_date, nc_end_date = gpm_download.validate_hdf()

    print(is_valid, nc_start_date, nc_end_date)

    gpm_download.process_data()

    # cat = get_catalog(date)

    # url = rf"https://gpm2.gesdisc.eosdis.nasa.gov/opendap/hyrax/GPM_L3/GPM_3IMERGHHL.07/{date.strftime('%Y')}/{date.strftime('%j')}/catalog.xml"
    # print(url)

    # ds = nc.Dataset(gpm_retrieval_url)

    # imerg_bounds = (cosmo_bounds[0] + 180, cosmo_bounds[1] + 90, cosmo_bounds[2] + 180, cosmo_bounds[3] + 90)
    # print(gpm_retrieval_url)

    # ds = nc.Dataset(gpm_retrieval_url)
    # p
