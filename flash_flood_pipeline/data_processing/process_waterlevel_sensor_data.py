from datetime import datetime, timedelta
import pandas as pd
import os
import json
from settings.base import (
    WATERLEVEL_SENSOR,
    METEO_RAIN_SENSOR,
)
import logging

logger = logging.getLogger(__name__)


def process_waterlevel_sensor_data():
    gauges_data_files = os.listdir("data/gauge_data")
    month = datetime.now().month
    gauges_actual_data_dict = {}
    gauges_reference_value_dict = {}
    gauges_yesterday_dict = {}
    yesterday = datetime.now() - timedelta(days=1)

    karonga_sensor_data_files = [x for x in gauges_data_files if x.endswith(".txt")]
    datetime_list = [
        datetime.strptime(x.split("_")[1], "%y%m%d%H%M%S.txt")
        for x in karonga_sensor_data_files
    ]
    karonga_latest_file = karonga_sensor_data_files[-1]
    karonga_file_yesterday = (
        "data/gauge_data/Karonga_"
        + datetime.strftime(
            min(datetime_list, key=lambda d: abs(d - yesterday)), "%y%m%d%H%M%S"
        )
        + ".txt"
    )
    reference_values = pd.read_csv("data/gauge_data/sensor_info_karonga.csv")
    fid = int(reference_values.loc[reference_values["key"] == "fid", "value"].item())

    gauges_reference_value_dict[fid] = reference_values.loc[
        reference_values["key"] == str(month), "value"
    ].item()

    karonga_yesterday = pd.read_csv(
        karonga_file_yesterday,
        delimiter=";",
        skiprows=1,
        usecols=[1, 3],
        header=None,
        names=["time", "distance"],
    )

    gauges_yesterday_dict[fid] = (
        karonga_yesterday["distance"].iloc[-1]
        + reference_values.loc[reference_values["key"] == "elevation", "value"].item()
        + reference_values.loc[
            reference_values["key"] == "sensor_height", "value"
        ].item()
    )

    karonga_today = pd.read_csv(
        "data/gauge_data/" + karonga_latest_file,
        delimiter=";",
        skiprows=1,
        usecols=[1, 3],
        header=None,
        names=["time", "distance"],
    )

    gauges_actual_data_dict[fid] = (
        karonga_today["distance"].iloc[-1]
        + reference_values.loc[reference_values["key"] == "elevation", "value"].item()
        + reference_values.loc[
            reference_values["key"] == "sensor_height", "value"
        ].item()
    )

    for sensor in WATERLEVEL_SENSOR:
        sensor_filtered_list = [
            x for x in gauges_data_files if x.endswith(str(sensor) + ".json")
        ]
        datetime_list = [
            datetime.strptime(x.split("_")[0], "%Y%m%d%H%M%S")
            for x in sensor_filtered_list
        ]
        file_yesterday = (
            "data/gauge_data/"
            + datetime.strftime(
                min(datetime_list, key=lambda d: abs(d - yesterday)), "%Y%m%d%H%M%S"
            )
            + "_"
            + str(sensor)
            + ".json"
        )

        data_file_today = sensor_filtered_list[-1]

        with open("data/gauge_data/" + data_file_today) as json_file:
            data = json.load(json_file)

        last_non_empty = [i for i in data["data"] if "Wlev" in i][-1]
        reference_values = pd.read_csv(
            "data/gauge_data/sensor_info_{}.csv".format(str(sensor))
        )
        fid = reference_values.loc[reference_values["key"] == "fid", "value"].item()
        if isinstance(last_non_empty["Wlev"], str):
            waterlevel = float(last_non_empty["Wlev"].strip("*T"))
        else:
            waterlevel = float(last_non_empty["Wlev"])
        gauges_actual_data_dict[fid] = (
            waterlevel
            + reference_values.loc[
                reference_values["key"] == "elevation", "value"
            ].item()
            + reference_values.loc[
                reference_values["key"] == "sensor_height", "value"
            ].item()
        )
        gauges_reference_value_dict[fid] = reference_values.loc[
            reference_values["key"] == str(month), "value"
        ].item()
        with open(file_yesterday) as json_file:
            data = json.load(json_file)
        last_non_empty_yesterday = [i for i in data["data"] if "Wlev" in i][-1]
        if isinstance(last_non_empty_yesterday["Wlev"], str):
            waterlevel = float(last_non_empty_yesterday["Wlev"].strip("*T"))
        else:
            waterlevel = float(last_non_empty_yesterday["Wlev"])
        gauges_yesterday_dict[fid] = (
            waterlevel
            + reference_values.loc[
                reference_values["key"] == "elevation", "value"
            ].item()
            + reference_values.loc[
                reference_values["key"] == "sensor_height", "value"
            ].item()
        )
    return (
        gauges_actual_data_dict,
        gauges_reference_value_dict,
        gauges_yesterday_dict,
    )
