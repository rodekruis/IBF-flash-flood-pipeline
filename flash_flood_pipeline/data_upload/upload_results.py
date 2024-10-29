import logging
import datetime
from settings.base import (
    ALERT_THRESHOLD_PARAMETER,
    ALERT_THRESHOLD_VALUE,
    COUNTRY_CODE_ISO3,
    DISASTER_TYPE,
)
from mapping_tables.exposure_mapping_tables import (
    EXPOSURE_TYPES,
    TA_EXPOSURE_DICT,
    POINT_EXPOSURE_DICT,
    DYNAMIC_POINT_EXPOSURE_DICT,
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
        to the IBF-portal with the dynamic indicator "alert_threshold" = 1
        """
        for key, value in EXPOSURE_TYPES.items():
            exposure_df = self.TA_exposure.astype({key: "float"}).astype({key: "int"})
            body = TA_EXPOSURE_DICT
            body["dynamicIndicator"] = value
            body["leadTime"] = self.lead_time
            body["eventName"] = self.district_name
            body["exposurePlaceCodes"] = (
                exposure_df[["placeCode", key]]
                .rename(columns={key: "amount"})
                .dropna()
                .to_dict("records")
            )
            body["date"] = self.date.strftime("%Y-%m-%dT%H:%M:%SZ")
            api_post_request("admin-area-dynamic-data/exposure", body=body)

        df_triggered_tas = self.TA_exposure[["placeCode", ALERT_THRESHOLD_PARAMETER]]
        # df_triggered_tas = df_triggered_tas.fillna()
        for index, row in df_triggered_tas.iterrows():
            if row[ALERT_THRESHOLD_PARAMETER] is None:
                df_triggered_tas.at[index, "trigger_value"] = 0
            elif row[
                ALERT_THRESHOLD_PARAMETER
            ] > ALERT_THRESHOLD_VALUE and self.lead_time not in [
                "24-hour",
                "48-hour",
            ]:  # double check implementation
                df_triggered_tas.at[index, "trigger_value"] = 1
            else:
                df_triggered_tas.at[index, "trigger_value"] = 0
        body = TA_EXPOSURE_DICT
        body["dynamicIndicator"] = "alert_threshold"
        body["leadTime"] = self.lead_time
        body["eventName"] = self.district_name
        body["exposurePlaceCodes"] = (
            df_triggered_tas[["placeCode", "trigger_value"]]
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
            dynamic_post_body = {
                "pointDataCategory": point_data_category,
                "leadTime": self.lead_time,
                "key": "exposure",
                "dynamicPointData": [{int(fid): True} for fid in exposed_fids],
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
        logger.info(exposed_roads_body)
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
        logger.info(exposed_buildings_body)
        api_post_request("lines-data/exposure-status", body=exposed_buildings_body)

    def send_notifications(self):
        """
        Send notification email/whatsapp for all triggered areas by posting to notification/send endpoint
        """
        body = {
            "countryCodeISO3": COUNTRY_CODE_ISO3,
            "disasterType": DISASTER_TYPE,
        }
        api_post_request("notification/send", body=body)

    def upload_sensor_values(self):
        """
        upload sensor values to IBF system. Uses the point-data/dynamic endpoint to send:
        - current sensor value
        - yesterday's sensor value
        - a reference value typical for the time of year.
        """
        sensor_values_body = POINT_EXPOSURE_DICT
        sensor_values_body["leadTime"] = "1-hour"
        sensor_values_body["dynamicIndicator"] = "water-level"
        sensor_values_body["pointDataCategory"] = "gauges"
        sensor_values_body["key"] = "water-level"

        values_list = []

        for key, value in self.sensor_actual_values_dict.items():
            values_list.append({"fid": key, "value": value})
        sensor_values_body["dynamicPointData"] = values_list
        api_post_request("point-data/dynamic", body=sensor_values_body)

        sensor_values_body["key"] = "water-level-previous"
        values_list = []
        for key, value in self.sensor_previous_values_dict.items():
            values_list.append({"fid": key, "value": value})
        sensor_values_body["dynamicPointData"] = values_list
        api_post_request("point-data/dynamic", body=sensor_values_body)

        sensor_values_body["key"] = "water-level-reference"
        values_list = []
        for key, value in self.sensor_reference_values_dict.items():
            values_list.append({"fid": key, "value": value})
        sensor_values_body["dynamicPointData"] = values_list
        api_post_request("point-data/dynamic", body=sensor_values_body)

    def untrigger_portal(self):
        """
        Function to untriger the portal and set exposure values of all TA's to 0
        Function is called everytime the pipeline is executed but no trigger occurs.
        """
        body = TA_EXPOSURE_DICT
        body["dynamicIndicator"] = "alert_threshold"
        self.TA_exposure["amount"] = 0
        body["exposurePlaceCodes"] = (
            self.TA_exposure[["placeCode", "amount"]].dropna().to_dict("records")
        )
        body["leadTime"] = "1-hour"
        body["date"] = self.date.strftime("%Y-%m-%dT%H:%M:%SZ")
        api_post_request("admin-area-dynamic-data/exposure", body=body)

        body = TA_EXPOSURE_DICT
        body["dynamicIndicator"] = "population_affected"
        self.TA_exposure["amount"] = 0
        body["exposurePlaceCodes"] = (
            self.TA_exposure[["placeCode", "amount"]].dropna().to_dict("records")
        )
        body["leadTime"] = "1-hour"
        body["date"] = self.date.strftime("%Y-%m-%dT%H:%M:%SZ")
        api_post_request("admin-area-dynamic-data/exposure", body=body)
