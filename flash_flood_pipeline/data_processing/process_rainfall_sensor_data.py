from datetime import datetime, timedelta
import pandas as pd
import os
import json

from settings.base import (
    BLANTYRE_RAINFALL_SENSORS,
)
from itertools import compress
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def download_rainfall_sensor_data():
    start_date = datetime.now()
    archive_location = Path(r"data/gauge_data")

    start_sensor_date = start_date - timedelta(days=7)
    
    rain_sensor_data_collection = []
    
    for gauge_name, gauge_id in BLANTYRE_RAINFALL_SENSORS.items():
        if gauge_name != "blantyre_waterboard_reservoir":
            print(gauge_name, gauge_id)
            gauge_files = [fn for fn in archive_location.glob(f"*{gauge_id}.json")]

            dataframe_entry_list = []

            for gauge_file in gauge_files:
                with open(gauge_file, "r") as src:
                    f = json.load(src)
                    rainfall_data = [
                        data_entry for data_entry in f["data"] if "Rain" in data_entry
                    ]

                    for rainfall_ts in rainfall_data:
                        dataframe_entry_list.append(
                            pd.Series(
                                data={
                                    "rainfall": rainfall_ts["Rain"],
                                },
                                name=datetime.strptime(
                                    str(rainfall_ts["$ts"]), "%y%m%d%H%M%S"
                                ),
                            )
                        )

            sensor_data = pd.concat(dataframe_entry_list, axis=1).T          
            #print(sensor_data)
            sensor_data = sensor_data.reset_index()
            sensor_data = sensor_data.drop_duplicates().set_index("index")

            for col in sensor_data.columns:
                sensor_data[col] = pd.to_numeric(sensor_data[col], errors="coerce")
     
            sensor_data = sensor_data.sort_index()
            
            sensor_data["rainfall_prev"] = sensor_data.shift(1)[["rainfall"]]
            sensor_data = sensor_data.fillna(0)
            sensor_data[f"{gauge_name}_rain"] = (
                sensor_data["rainfall"] - sensor_data["rainfall_prev"]
            )
            if sensor_data.index[0] >= start_sensor_date:
                sensor_data.loc[sensor_data.index[0], f"{gauge_name}_rain"] = 0
            print(sensor_data.head())
            print(start_sensor_date)
            sensor_data = sensor_data.loc[sensor_data.index >= start_sensor_date]
            print(sensor_data.head())
            sensor_data = sensor_data.sort_index()
            
            sensor_data = sensor_data[[f"{gauge_name}_rain"]]
            print(sensor_data.head())
            rain_sensor_data_collection.append(sensor_data)
        
    gauge_rainfall = pd.concat(rain_sensor_data_collection, axis=1)
    return gauge_rainfall


    
