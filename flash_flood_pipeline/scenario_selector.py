import pandas as pd
from datetime import datetime
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
)

COLUMNAME = "precipitation"

EVENT_TRIGGER_HOURS = [
    0,
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    15,
    18,
    21,
    24,
    48,
]

EVENT_SEVERITY_ORDER = [
    "5mm_1hr",
    "10mm_12hr",
    "10mm_4hr",
    "10mm_2hr",
    "20mm_12hr",
    "20mm_4hr",
    "10mm_1hr",
    "20mm_2hr",
    "15mm_1hr",
    "30mm_12hr",
    "50mm_48hr",
    "50mm_24hr",
    "30mm_4hr",
    "30mm_2hr",
    "20mm_1hr",
    "25mm_1hr",
    "40mm_12hr",
    "40mm_4hr",
    "40mm_2hr",
    "30mm_1hr",
    "75mm_24hr",
    "100mm_48hr",
    "50mm_12hr",
    "60mm_12hr",
    "50mm_4hr",
    "50mm_2hr",
    "35mm_1hr",
    "40mm_1hr",
    "100mm_24hr",
    "70mm_12hr",
    "80mm_12hr",
    "60mm_4hr",
    "60mm_2hr",
    "45mm_1hr",
    "50mm_1hr",
    "90mm_12hr",
    "70mm_4hr",
    "70mm_2hr",
    "125mm_24hr",
    "150mm_48hr",
    "100mm_12hr",
    "150mm_24hr",
    "200mm_48hr",
    "200mm_24hr",
]

UPSTREAM_MAP = {
    "MW10410": ["MW10410"],
    "MW10220": ["MW10220", "MW10203", "MW10104", "MW10106"],
    "MW10203": ["MW10203", "MW10104", "MW10106"],
    "MW10104": ["MW10104", "MW10106"],
    "MW10106": ["MW10106"],
    "MW10411": ["MW10411"],
    "MW10503": ["MW10503"],
    "MW10506": ["MW10506"],
    "MW10502": ["MW10502", "MW10503"],
    "MW10520": ["MW10520", "MW10506"],
    "MW10510": ["MW10510", "MW10506"],
    "MW10501": ["MW10501", "MW10510", "MW10506", "MW10520"],
    "MW10505": ["MW10505", "MW10501", "MW10506", "MW10510", "MW10520"],
    "MW10509": [
        "MW10509",
        "MW10505",
        "MW10501",
        "MW10506",
        "MW10510",
        "MW10520",
        "MW10511",
        "MW10411",
    ],
    "MW10511": ["MW10511", "MW10411"],
    "MW10401": [
        "MW10401",
        "MW10509",
        "MW10505",
        "MW10501",
        "MW10506",
        "MW10510",
        "MW10520",
        "MW10511",
        "MW10411",
    ],
    "MW10420": [
        "MW10420",
        "MW10410",
        "MW10401",
        "MW10509",
        "MW10505",
        "MW10501",
        "MW10506",
        "MW10510",
        "MW10520",
        "MW10511",
        "MW10411",
    ],
    "MW10504": [
        "MW10504",
        "MW10502",
        "MW10503",
        "MW10420",
        "MW10410",
        "MW10401",
        "MW10509",
        "MW10505",
        "MW10501",
        "MW10506",
        "MW10510",
        "MW10520",
        "MW10511",
        "MW10411",
    ],
    "MW10407": [
        "MW10407",
        "MW10504",
        "MW10502",
        "MW10503",
        "MW10420",
        "MW10410",
        "MW10401",
        "MW10509",
        "MW10505",
        "MW10501",
        "MW10506",
        "MW10510",
        "MW10520",
        "MW10511",
        "MW10411",
    ],
    "MW10403": ["MW10403"],
    "MW10404": ["MW10404"],
    "MW10402": [
        "MW10402",
        "MW10404",
        "MW10403",
        "MW10407",
        "MW10504",
        "MW10502",
        "MW10503",
        "MW10420",
        "MW10410",
        "MW10401",
        "MW10509",
        "MW10505",
        "MW10501",
        "MW10506",
        "MW10510",
        "MW10520",
        "MW10511",
        "MW10411",
    ],
    "MW31546": ["MW31546"],
    "MW31545": ["MW31545"],
    "MW31541": ["MW31541"],
    "MW31548": ["MW31548"],
    "MW31552": ["MW31552"],
    "MW31540": ["MW31540"],
    "MW31549": ["MW31549"],
    "MW31543": ["MW31543"],
    "MW31533": ["MW31533"],
    "MW31539": ["MW31539"],
    "MW31531": ["MW31531"],
    "MW31553": ["MW31553"],
    "MW31544": ["MW31544"],
    "MW31542": ["MW31542"],
    "MW31551": ["MW31551"],
    "MW31537": ["MW31537"],
    "MW31536": ["MW31536"],
    "MW31535": ["MW31535"],
    "MW31534": ["MW31534"],
    "MW31538": ["MW31538"],
    "MW31547": ["MW31547"],
    "MW31550": ["MW31550"],
    "MW31532": ["MW31532"],
}


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
            df["48hr"] = df[COLUMNAME].rolling("48H").sum()
            df["24hr"] = df[COLUMNAME].rolling("24H").sum()
            df["12hr"] = df[COLUMNAME].rolling("12H").sum()
            df["4hr"] = df[COLUMNAME].rolling("4H").sum()
            df["2hr"] = df[COLUMNAME].rolling("2H").sum()
            df["1hr"] = df[COLUMNAME].rolling("1H").sum()
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
                df[column] = df[column].astype(int).astype(str) + "mm_" + str(column)
        return gfs_data

    def find_worst_event(self, df_target_hours):
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
                severity_index_list.append(EVENT_SEVERITY_ORDER.index(item))
        if severity_index_list:
            most_severe_event = EVENT_SEVERITY_ORDER[max(severity_index_list)]
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
            event, leadtime = self.find_worst_event(df_target_hours)
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
