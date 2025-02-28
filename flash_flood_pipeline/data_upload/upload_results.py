import logging
import datetime
import pandas as pd
from settings.base import (
    ALERT_THRESHOLD_PARAMETER,
    ALERT_THRESHOLD_VALUE,
    COUNTRY_CODE_ISO3,
    DISASTER_TYPE,
    KARONGA_PLACECODES,
    RUMPHI_PLACECODES,
    BLANTYRE_PLACECODES,
    THRESHOLD_CORRECTION_VALUES,
)
from mapping_tables.exposure_mapping_tables import (
    EXPOSURE_TYPES,
    TA_EXPOSURE_DICT,
    GEOSERVER_EXPOSURE_DICT,
)
from utils.api import api_post_request


logger = logging.getLogger(__name__)


class DataUploader:
    def __init__(
        self,
        time,
        regions,
        district_name,
        schools,
        waterpoints,
        roads,
        buildings,
        health_sites,
        sensor_actual_values_dict={},
        sensor_previous_values_dict={},
        sensor_reference_values_dict={},
        date=datetime.datetime.now(),
    ):
        """Class to upload forecasted flooding for all vector datasets (e.g., schools, waterpoints, TA's) and to upload sensor values. Raster uploads are done through a separate
        class (RasterUploader)

        Args:
            time (str): leadtime in string format as expected by the IBF-portal: "1-hour"
            regions (gpd.GeoDataFrame): dataframe with all TA's including their exposure values (e.g., nr of affected people) for this district
            district_name (str): Name used in the trigger warning (e.g. "Rumphi" or "Karonga")
            schools (gpd.GeoDataFrame, points): all schools in the TA's concerned with their exposure status (no risk, moderate risk, high risk)
            waterpoints (gpd.GeoDataFrame, points): all waterpoints in the TA's concerned with their exposure status (no risk, moderate risk, high risk)
            roads (gpd.GeoDataFrame, lines): all roads in the TA's concerned with their exposure status (no risk, moderate risk, high risk)
            buildings (gpd.GeoDataFrame, polygons): all buildings in the TA's concerned with their exposure status (no risk, moderate risk, high risk)
            health_sites (gpd.GeoDataFrame, points): all health_sites in the TA's concerned with their exposure status (no risk, moderate risk, high risk)
            sensor_actual_values_dict (Dict): Dictionary with sensor id in the IBF portal as key and the latest measured value as value.
            sensor_previous_values_dict (Dict): Dictionary with sensor id in the IBF portal as key and the measured value of 24 hours ago as value.
            sensor_reference_values_dict (Dict): Dictionary with sensor id in the IBF portal as key and the Reference (typical) value for this month as value.
            date (datetime.datetime): Reference datetime to be send to the IBF portal
        """
        self.TA_exposure = regions
        self.schools_exposure = schools
        self.waterpoints_exposure = waterpoints
        self.roads_exposure = roads
        self.buildings_exposure = buildings
        self.health_sites_exposure = health_sites
        self.lead_time = time
        self.district_name = district_name
        self.date = date
        self.sensor_reference_values_dict = sensor_reference_values_dict
        self.sensor_actual_values_dict = sensor_actual_values_dict
        self.sensor_previous_values_dict = sensor_previous_values_dict

    def upload_and_trigger_tas(self):
        """
        (1) Uploading all values for the different exposure types (exposed population, estimation of damage, nr of affected roads,
        nr of affected schools, nr of affected clinics, nr of affected waterpoints, nr of affected buildings) for each TA by
        sending an API request (endpoint: admin-area-dynamic-data/exposure) and (2) triggering a TA when the value of the trigger
        exposure type is larger than the trigger threshold value (nr people affected >20), by sending an API-request (endpoint: admin-area-dynamic-data/exposure)
        to the IBF-portal
        """
        ta_exposure_trigger = self.TA_exposure.copy()

        # 1 = trigger
        # 0 + event name = warning
        # 0 = no event

        def determine_ta_trigger_state(row):
            if row["placeCode"] in THRESHOLD_CORRECTION_VALUES:
                threshold = int(
                    ALERT_THRESHOLD_VALUE
                    + THRESHOLD_CORRECTION_VALUES.get(row["placeCode"])
                )

            else:
                threshold = ALERT_THRESHOLD_VALUE

            if row[ALERT_THRESHOLD_PARAMETER] > threshold and self.lead_time not in [
                "15-hour",
                "18-hour",
                "21-hour",
                "24-hour",
                "48-hour",
            ]:
                return 1

            elif row[ALERT_THRESHOLD_PARAMETER] > threshold and self.lead_time in [
                "15-hour",
                "18-hour",
                "21-hour",
                "24-hour",
                "48-hour",
            ]:
                return 0
            else:
                return None

        ta_exposure_trigger["trigger_value"] = ta_exposure_trigger.apply(
            lambda row: determine_ta_trigger_state(row), axis=1
        )

        ta_exposure_trigger = ta_exposure_trigger.loc[
            ~pd.isnull(ta_exposure_trigger["trigger_value"])
        ]  # filter all that are not warning or trigger

        df_triggered_tas_blantyre = ta_exposure_trigger.loc[
            ta_exposure_trigger["placeCode"].isin(BLANTYRE_PLACECODES)
        ]
        df_triggered_tas_karonga = ta_exposure_trigger.loc[
            ta_exposure_trigger["placeCode"].isin(KARONGA_PLACECODES)
        ]
        df_triggered_tas_rumphi = ta_exposure_trigger.loc[
            ta_exposure_trigger["placeCode"].isin(RUMPHI_PLACECODES)
        ]
        event_mapping = {
            "Blantyre City": df_triggered_tas_blantyre,
            "Karonga": df_triggered_tas_karonga,
            "Rumphi": df_triggered_tas_rumphi,
        }
        

        for distr_name, exposed_tas in event_mapping.items():
            if len(exposed_tas) > 0:
                for key, value in EXPOSURE_TYPES.items():
                    exposure_df = exposed_tas.astype({key: "float"}).astype(
                        {key: "int"}
                    )
                    exposure_df[key] = exposure_df.apply(
                        lambda row: 0 if row[key] < 0 else row[key], axis=1
                    )
                    body = TA_EXPOSURE_DICT
                    body["dynamicIndicator"] = value
                    body["leadTime"] = self.lead_time
                    body["eventName"] = distr_name
                    body["exposurePlaceCodes"] = (
                        exposure_df[["placeCode", key]]
                        .rename(columns={key: "amount"})
                        .dropna()
                        .to_dict("records")
                    )
                    body["date"] = self.date.strftime("%Y-%m-%dT%H:%M:%SZ")
                    api_post_request("admin-area-dynamic-data/exposure", body=body)

                for _, row in exposed_tas.iterrows():
                    if row["trigger_value"] == 1:
                        post_type = "trigger"
                    elif row["trigger_value"] == 0:
                        post_type = " warning"
                    else:
                        post_type = None
                    logger.info(
                        f"Posting: {distr_name} - leadtime = {self.lead_time} | {row['placeCode']} | {post_type}"
                    )
                                
                # forecast_severity: upload value=1 for all warned or triggered areas
                body = TA_EXPOSURE_DICT
                body["dynamicIndicator"] = "forecast_severity"
                body["leadTime"] = self.lead_time
                body["eventName"] = distr_name
                body["exposurePlaceCodes"] = (
                    exposed_tas[["placeCode"]]
                    .assign(amount=1) # assuming this works, as non-warned/non-triggered areas are not part of this dict
                    .dropna()
                    .to_dict("records")
                )
                body["date"] = self.date.strftime("%Y-%m-%dT%H:%M:%SZ")
                api_post_request("admin-area-dynamic-data/exposure", body=body)

                # forecast_trigger
                body = TA_EXPOSURE_DICT
                body["dynamicIndicator"] = "forecast_trigger"
                body["leadTime"] = self.lead_time
                body["eventName"] = distr_name
                body["exposurePlaceCodes"] = (
                    exposed_tas[["placeCode", "trigger_value"]]
                    .rename(columns={"trigger_value": "amount"})
                    .dropna()
                    .to_dict("records")
                )
                body["date"] = self.date.strftime("%Y-%m-%dT%H:%M:%SZ")
                api_post_request("admin-area-dynamic-data/exposure", body=body)

    def expose_point_assets(self):
        """
        Mark point assets (schools, waterpoints, health sites) with a high risk as exposed in the IBF portal.
        All exposed assets are identified and their id value is posted to the ibf-portal by using the endpoint:
        point-data/dynamic
        """
        exposed_schools = list(
            self.schools_exposure.loc[
                self.schools_exposure["vulnerability"] == "high risk", "id"
            ]
        )

        exposed_waterpoints = list(
            self.waterpoints_exposure.loc[
                self.waterpoints_exposure["vulnerability"] == "high risk", "id"
            ]
        )

        exposed_healthsites = list(
            self.health_sites_exposure.loc[
                self.health_sites_exposure["vulnerability"] == "high risk", "id"
            ]
        )

        for point_data_category, exposed_fids in {
            "schools": exposed_schools,
            "waterpoints_internal": exposed_waterpoints,
            "health_sites": exposed_healthsites,
        }.items():
            if exposed_fids:
                dynamic_post_body = {
                    "pointDataCategory": point_data_category,
                    "disasterType": "flash-floods",
                    "leadTime": self.lead_time,
                    "key": "exposure",
                    "dynamicPointData": [
                        {"fid": int(fid), "value": True} for fid in exposed_fids
                    ],
                }
                api_post_request("point-data/dynamic", body=dynamic_post_body)

    def expose_geoserver_assets(self):
        """
        Mark features in large geographic datasets (currently buildings & roads) as exposed in the IBF portal. These have a different endpoint then the point assets
        because they are visualized and administered differently by the IBF-portal. High risk assets are exposed by sending their id to the lines-data/exposure-status endpoint.
        """
        exposed_roads = list(
            self.roads_exposure.loc[
                self.roads_exposure["vulnerability"] == "high risk", "id"
            ]
        )
        exposed_roads = [str(int(x)) for x in exposed_roads]
        exposed_roads_body = GEOSERVER_EXPOSURE_DICT
        exposed_roads_body["exposedFids"] = exposed_roads
        exposed_roads_body["linesDataCategory"] = "roads"
        exposed_roads_body["leadTime"] = self.lead_time
        # logger.info(exposed_roads_body)
        api_post_request("lines-data/exposure-status", body=exposed_roads_body)

        exposed_buildings = list(
            self.buildings_exposure.loc[
                self.buildings_exposure["vulnerability"] == "high risk", "id"
            ]
        )
        exposed_buildings = [str(int(x)) for x in exposed_buildings]
        exposed_buildings_body = GEOSERVER_EXPOSURE_DICT
        exposed_buildings_body["exposedFids"] = exposed_buildings
        exposed_buildings_body["linesDataCategory"] = "buildings"
        exposed_buildings_body["leadTime"] = self.lead_time
        # logger.info(exposed_buildings_body)
        api_post_request("lines-data/exposure-status", body=exposed_buildings_body)

    def upload_sensor_values(self):
        """
        upload sensor values to IBF system. Uses the point-data/dynamic endpoint to send:
        - current sensor value
        - yesterday's sensor value
        - a reference value typical for the time of year.
        """
        for sensor_dict, post_key in [
            (self.sensor_actual_values_dict, "water-level"),
            (self.sensor_previous_values_dict, "water-level-previous"),
            (self.sensor_reference_values_dict, "water-level-reference"),
        ]:
            values_list = []

            for key, value in sensor_dict.items():
                values_list.append({"fid": int(key), "value": value})

            sensor_dynamic_body = {
                "date": self.date.strftime(format="%Y-%m-%dT%H:%M:%S.%fZ"),
                "leadTime": self.lead_time,
                "key": post_key,
                "countryCodeISO3": "MWI",
                "disasterType": "flash-floods",
                "pointDataCategory": "gauges",
                "dynamicPointData": values_list,
            }
            api_post_request("point-data/dynamic", body=sensor_dynamic_body)

    def untrigger_portal(self):
        """
        Function to untriger the portal and set exposure values of all TA's to 0
        Function is called everytime the pipeline is executed but no trigger occurs.
        """

        untrigger_ta = self.TA_exposure.copy()
        untrigger_ta["amount"] = 0

        body = TA_EXPOSURE_DICT
        body["dynamicIndicator"] = "population_affected"  # "population_affected"

        body["exposurePlaceCodes"] = (
            untrigger_ta[["placeCode", "amount"]].dropna().to_dict("records")
        )
        body["leadTime"] = "1-hour"
        body["date"] = self.date.strftime("%Y-%m-%dT%H:%M:%SZ")
        body["eventName"] = None

        api_post_request("admin-area-dynamic-data/exposure", body=body)

        # upload 'forecast_severity' with value 0 for all TA's
        body = TA_EXPOSURE_DICT
        body["dynamicIndicator"] = "forecast_severity"
        body["exposurePlaceCodes"] = (
            untrigger_ta[["placeCode", "amount"]].dropna().to_dict("records")
        )
        body["leadTime"] = "1-hour"
        body["date"] = self.date.strftime("%Y-%m-%dT%H:%M:%SZ")
        body["eventName"] = None

        api_post_request("admin-area-dynamic-data/exposure", body=body)
        
        # upload 'forecast_trigger' with value 0 for all TA's
        body = TA_EXPOSURE_DICT
        body["dynamicIndicator"] = "forecast_trigger"
        body["exposurePlaceCodes"] = (
            untrigger_ta[["placeCode", "amount"]].dropna().to_dict("records")
        )
        body["leadTime"] = "1-hour"
        body["date"] = self.date.strftime("%Y-%m-%dT%H:%M:%SZ")
        body["eventName"] = None

        api_post_request("admin-area-dynamic-data/exposure", body=body)
