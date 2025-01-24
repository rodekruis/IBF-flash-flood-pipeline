from utils.iridium_utils.encode_date import encode_date
from utils.iridium_utils.format_datetime import format_datetime
from utils.iridium_utils.format_datetime_file_format import (
    format_datetime_file_format,
)
import datetime
import math
import os
from data_download.get_gauge_from_gmail import get_satellite_data

MAX_COMP_BYTES = 9


def gather_satellite_data():
    filename_list = get_satellite_data()
    for filename in filename_list:
        with open(filename, "br") as file:
            data = file.read()
            process_compacted_data(r"data/gauge_data", data, len(data), "Karonga")


def process_compacted_data(export_path, log_data, log_data_size, emei):
    """
    For our sensor in Karonga we use a satellite modem to transfer the measurement data to the IBF system. To reduce costs the sensor sends out a compacted byte sequence. This function is able to extract the compacted data.

    Args:
        export_path (str): folder to store the extracted sbd file to
        log_data (Bytes): Bytestring as read from the sbd file
        log_data_size (int): number of bytes that make up the file
        emei (int): obsolete parameter used to be needed for extraction of bytes

    Returns:
        True when a file has been decripted succesfully and False if not.
    """
    if log_data_size == 0:
        return True  # Nothing to do

    if log_data[0] == ord("*"):
        return False  # Not compacted data

    # data_list = []  # Equivalent to TStringList in C++
    # parts_list = []  # Equivalent to TStringList in C++

    data_str = "L;MSG-SIZE;Message size;byte"

    if True:  # Compacted data starts with a TS record
        values = [0.0] * 16
        values[0] = encode_date(2017, 1, 1)  # Time since 1-jan-2017

        index = 0
        cnt = 0

        while index < log_data_size:
            ui_par = (log_data[index] >> 4) & 0x0F  # Parameter number
            value = 0.0
            multiplier = 1.0

            factor = 1.0 if ui_par == 0 else math.pow(10, log_data[index] & 0x07)
            negative = (log_data[index] & 0x08) == 0x08
            single_byte = False

            if ui_par == 0:  # TS
                multiplier = {
                    0: 1.0,  # Days
                    1: 1.0 / 24.0,  # Hours
                    2: 1.0 / 1440.0,  # Minutes
                }.get(
                    log_data[index] & 0x03, 1.0 / 86400.0
                )  # Seconds

                if (log_data[index] & 0x04) == 0x04:
                    value = multiplier
                    single_byte = True

            index += 1

            for cnt in range(1, MAX_COMP_BYTES):
                if single_byte:
                    break
                value += (log_data[index] & 0x7F) * multiplier
                index += 1
                if (log_data[index - 1] & 0x80) == 0:
                    break
                multiplier *= 128.0

            if negative:
                values[ui_par] = values[ui_par] - (value / factor)
            else:
                values[ui_par] = values[ui_par] + (value / factor)

            if ui_par == 0:
                data_str += "\rD;" + format_datetime(
                    datetime.datetime.fromtimestamp(values[0] * 86400.0 + 1483225200)
                )  # Convert days back to datetime
            elif values[0] > 0:  # First a valid TS
                data_str += f";P{ui_par:02};{values[ui_par]}"

        if values[0] > 0:  # Must be at least a valid TS
            data_str += f";MSG-SIZE;{log_data_size}"
            last_timestamp = datetime.datetime.fromtimestamp(
                values[0] * 86400.0 + 1483225200
            )

            try:
                file_name = os.path.join(
                    export_path,
                    f"Karonga_{format_datetime_file_format(last_timestamp)}.txt",
                )
                with open(file_name, "w") as file:
                    file.write(data_str)
            except Exception as e:
                print(f"Error: {e}")
                return False

    return True
