import pandas as pd
from datetime import datetime
from utils.general_utils.convert_placecode_to_district import (
    convert_placecode_to_district,
)

import numpy as np
from mapping_tables.event_mapping import (
    event_mapping_12hr,
    event_mapping_24hr,
    event_mapping_48hr,
    event_mapping_4hr,
    event_mapping_2hr,
    event_mapping_1hr,
)
from settings.base import (
    KARONGA_PLACECODES,
    RUMPHI_PLACECODES,
    BLANTYRE_PLACECODES,
    SMALL_LAGTIME_PLACECODES,
    SEVERITY_ORDER_DISTRICT_MAPPING,
    EVENT_TRIGGER_HOURS,
    UPSTREAM_MAP,
)

COLUMNAME = "precipitation"


class scenarioSelector:
    """Scenario Selector class for the malawi IBF pipeline. The class converts rainfall per TA into flood
    forecast predictions. The flood forecasts are a pre-computed set of gis datasets for certain amounts of rainfall
    in a fixed time period. For TA's with a (substantial) upstream area, the precipitation over the entire upstream area is
    taken into account.
    """

    def __init__(self, gfs_data):
        self.gfs_data = gfs_data

    def add_rolling_functions(self):
        """
        Add for each datetime in the dataframe a new column with the sum of the values before that datetime and 48H, 24H, 12H, 4H, 2H and 1H before.

        Args:
            gfs_data (dict): dictionary of the gfs_data with PlaceCode TA as key and dataframe as value including precipitation column

        Returns:
            gfs_data (dict): dictionary of the gfs_data with PlaceCode TA as key and dataframe as value. Rolling summation values of 48H, 24H, 12H, 4H, 2H and 1H before are added as columns (e.g., 1hr column for hourly rolling sum).
        """
        gfs_data = self.gfs_data.copy()
        for _, df in gfs_data.items():
            df["datetime"] = pd.to_datetime(df["datetime"])
            df.set_index("datetime", inplace=True)
        for _, df in gfs_data.items():
            df["48hr"] = df[COLUMNAME].rolling("48h").sum()
            df["24hr"] = df[COLUMNAME].rolling("24h").sum()
            df["12hr"] = df[COLUMNAME].rolling("12h").sum()
            df["4hr"] = df[COLUMNAME].rolling("4h").sum()
            df["2hr"] = df[COLUMNAME].rolling("2h").sum()
            df["1hr"] = df[COLUMNAME].rolling("1h").sum()
            del df[COLUMNAME]
        return gfs_data

    def aggregate_upstream_tas(self, gfs_data):
        """
        For TA's with substantial upstream areas, their forecasted precipitation is averaged with upstream precipitation. This is done based
        on the rainfall forecast values for all TA's.

        Args:
            gfs_data (dict): dictionary of the gfs_data with PlaceCode TA as key and dataframe as value including precipitation column

        Returns:
            gfs_data (dict): dictionary of the gfs_data with PlaceCode TA as key and dataframe as value. The precipitation column is now
            an average of all relevant (upstream) TA's. For the TA mapping see settings.base.py
        """
        for key, value in gfs_data.items():
            upstream_tas = UPSTREAM_MAP[key]
            dfs = []
            for ta in upstream_tas:
                dfs.append(gfs_data[ta])

            averages = (
                pd.concat([each.stack() for each in dfs], axis=1)
                .apply(lambda x: x.mean(), axis=1)
                .unstack()
            )
            gfs_data[key] = averages
        return gfs_data

    def event_selection(self):
        """
        Map the cumulative rainfall to events present in our library, for example: 23 mm in 24 hours maps to 25 mm in 24 hours which is present
        in the library. Uses the dataframe with rainfall per traditional authority. This rainfall is first rolled and aggregated with upstream areas before selecting events (functions above)

        Returns:
            gfs_data (dict): dictionary of the gfs_data with PlaceCode TA as key and dataframe as value.
            In each dataframe for every temporal aggregation period (e.g., 1 hour, 2 hours) the corresponding event values are stored (e.g., 20mm_1hr)
        """
        gfs_data = self.add_rolling_functions()
        gfs_data = self.aggregate_upstream_tas(gfs_data)

        for key, df in gfs_data.items():
            for index, row in df.iterrows():

                df.loc[index, "12hr"] = event_mapping_12hr(row["12hr"].item())
                df.loc[index, "24hr"] = event_mapping_24hr(row["24hr"].item())
                df.loc[index, "48hr"] = event_mapping_48hr(row["48hr"].item())

            if key in SMALL_LAGTIME_PLACECODES:
                for index, row in df.iterrows():
                    df.loc[index, "1hr"] = event_mapping_1hr(row["1hr"].item())
                    df.loc[index, "2hr"] = event_mapping_2hr(row["2hr"].item())
                    df.loc[index, "4hr"] = event_mapping_4hr(row["4hr"].item())

            else:
                df.drop(columns=["1hr", "2hr", "4hr"], inplace=True)

            for column in df.columns:
                df[column] = (
                    df[column].astype(int).astype(str) + "mm_" + str(column)
                )

        return gfs_data

    def find_worst_event(self, df_target_hours, district):
        """
        Takes the dataframe with all events for a single TA and determines which event (e.g., 20mm in 1 hour) will give the largest flooding. The worst event (e.g., 40mm in 2 hours over 20mm in 1 hour) is stored and used to trigger/display in the IBF system.

        Args:
            df_target_hours (pd.DataFrame): Dataframe with rainfall intensities converted to events in the library for a single TA and only timestamps corresponding to IBF leadtimes.

        Returns:
            most_severe_event (str): name of the most severe event which will occur somewhere in the coming 48 hours
            event_time (int): time when the most severe event is going to occur. (in hours from now)
        """
        df_hours_filtered = df_target_hours.drop(columns=["time_reference"])
        events_list = df_hours_filtered.to_numpy().flatten()
        severity_index_list = []

        for item in events_list:
            if not str(item).startswith("0"):
                severity_index_list.append(
                    SEVERITY_ORDER_DISTRICT_MAPPING.get(district).index(item)
                )

        if severity_index_list:
            most_severe_event = SEVERITY_ORDER_DISTRICT_MAPPING.get(district)[
                max(severity_index_list)
            ]
            most_severe_event_index = df_target_hours[
                df_target_hours[most_severe_event.split("_")[1]] == most_severe_event
            ].first_valid_index()
            event_time = df_target_hours.loc[most_severe_event_index, "time_reference"]
            return most_severe_event, event_time
        else:
            return "0mm_1hr", 0

    def select_scenarios(self):
        """
        Determine the worst upcoming flood scenario for each TA in the coming 48 hours, including when the first flooding for any TA in a region occurs.

        Returns:
            karonga_leadtime (int): Timing of the first flood in karonga region (in hours from now)
            karonga_events (Dict): Dictionary with the TA's of Karonga as keys and their worst event as value "20mm_12hr" format. TA's without rain are excluded
            rumphi_leadtime (int): Timing of the first flood in rumphi region (in hours from now)
            rumphi_events (Dict): Dictionary with the TA's of rumphi as keys and their worst event as value "20mm_12hr" format. TA's without rain are excluded
            blantyre_leadtime (int): Timing of the first flood in blantyre region (in hours from now)
            blantyre_events (Dict): Dictionary with the TA's of blantyre as keys and their worst event as value "20mm_12hr" format. TA's without rain are excluded
        """
        event_data = self.event_selection()
        for _, df in event_data.items():
            df["time_reference"] = (
                (df.index - datetime.now()) / pd.Timedelta("1 hour")
            ).astype(int)

        karonga_leadtimes = []
        karonga_events = {}
        rumphi_leadtimes = []
        rumphi_events = {}
        blantyre_leadtimes = []
        blantyre_events = {}

        for key, df in event_data.items():
            df_target_hours = df[df["time_reference"].isin(EVENT_TRIGGER_HOURS)]
            event, leadtime = self.find_worst_event(
                df_target_hours=df_target_hours,
                district=convert_placecode_to_district(key),
            )

            if event not in [
                "0mm_1hr",
                "0mm_2hr",
                "0mm_4hr",
                "0mm_12hr",
                "0mm_24hr",
                "0mm_48hr",
            ]:
                if key in KARONGA_PLACECODES:
                    karonga_leadtimes.append(leadtime)
                    karonga_events[key] = event
                elif key in RUMPHI_PLACECODES:
                    rumphi_leadtimes.append(leadtime)
                    rumphi_events[key] = event
                elif key in BLANTYRE_PLACECODES:
                    blantyre_leadtimes.append(leadtime)
                    blantyre_events[key] = event
        if karonga_leadtimes:
            karonga_leadtime = min(karonga_leadtimes)
        else:
            karonga_leadtime = None
        if rumphi_leadtimes:
            rumphi_leadtime = min(rumphi_leadtimes)
        else:
            rumphi_leadtime = None
        if blantyre_leadtimes:
            blantyre_leadtime = min(blantyre_leadtimes)
        else:
            blantyre_leadtime = None
        return (
            karonga_leadtime,
            karonga_events,
            rumphi_leadtime,
            rumphi_events,
            blantyre_leadtime,
            blantyre_events,
        )
