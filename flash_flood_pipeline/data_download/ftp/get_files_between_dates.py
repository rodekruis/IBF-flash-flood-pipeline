from ftplib import FTP_TLS
from pathlib import Path
import datetime
import pandas as pd

FTP_HOST = "jsimpsonftps.pps.eosdis.nasa.gov"
IMERG_LATE_FTP_DIR = "data/imerg/late"

def get_remote_filepaths_in_daterange(
    username: str, start_date: datetime.datetime, end_date: datetime.datetime
) -> list[Path]:
    with FTP_TLS(FTP_HOST) as ftp:
        ftp.login(user=username, passwd=username)
        ftp.cwd(IMERG_LATE_FTP_DIR)
        folders = ftp.nlst()

        ftp_filelist = pd.DataFrame(data={"main_folder_names": folders})
        ftp_filelist["year"] = ftp_filelist.apply(
            lambda row: (
                datetime.datetime.strptime(row["main_folder_names"], "%Y%m").year
                if "V" not in row["main_folder_names"]
                else None
            ),
            axis=1,
        )
        ftp_filelist["month"] = ftp_filelist.apply(
            lambda row: (
                datetime.datetime.strptime(row["main_folder_names"], "%Y%m").month
                if "V" not in row["main_folder_names"]
                else None
            ),
            axis=1,
        )
        year_month_combos = list(
            set(
                [
                    (d.year, d.month)
                    for d in pd.date_range(start_date, end_date, freq="1d")
                ]
            )
        )

        filelist = []

        for cm in year_month_combos:
            ftp.cwd("/")
            ftp.cwd(IMERG_LATE_FTP_DIR)
            ftp.cwd(f"{cm[0]}{str(cm[1]).zfill(2)}")

            subfolder_files = ftp.nlst()

            subfolder_ftp_filelist = pd.DataFrame(
                data={"subfolder_filenames": subfolder_files}
            )
            subfolder_ftp_filelist["date"] = subfolder_ftp_filelist.apply(
                lambda row: (
                    datetime.datetime.strptime(
                        row["subfolder_filenames"].split("IMERG.")[-1].split("-")[0],
                        "%Y%m%d",
                    )
                    if "IMERG" in row["subfolder_filenames"]
                    else None
                ),
                axis=1,
            )

            subfolder_ftp_filelist_within_timeextent = subfolder_ftp_filelist.loc[
                (
                    subfolder_ftp_filelist["date"]
                    >= datetime.datetime.combine(start_date, datetime.time(0, 0, 0))
                )
                & (
                    subfolder_ftp_filelist["date"]
                    <= datetime.datetime.combine(end_date, datetime.time(0, 0, 0))
                )
            ]

            filepaths = [
                Path(IMERG_LATE_FTP_DIR) / f"{cm[0]}{str(cm[1]).zfill(2)}" / fname
                for fname in subfolder_ftp_filelist_within_timeextent[
                    "subfolder_filenames"
                ].tolist()
            ]
            filelist.extend(filepaths)

        return filelist