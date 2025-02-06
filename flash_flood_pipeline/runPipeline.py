import datetime
import time
from pathlib import Path
import geopandas as gpd
import numpy as np
import logging
from settings.base import (
    DATA_FOLDER,
    ASSET_TYPES,
    ENVIRONMENT,
    ALERT_THRESHOLD_VALUE,
    THRESHOLD_CORRECTION_VALUES,
)
from logger_config.configure_logger import configure_logger
from data_processing.process_compacted_iridium_data import gather_satellite_data
from data_processing.process_rainfall_sensor_data import (
    process_karonga_rainfall_sensor_data,
    process_blantyre_rainfall_sensor_data,
    blantyre_raingauge_idw,
)
from data_processing.process_waterlevel_sensor_data import (
    process_waterlevel_sensor_data,
)
from data_upload.upload_results import DataUploader
from data_upload.raster_uploader import RasterUploader
from utils.raster_utils.clip_rasters_on_ta import clip_rasters_on_ta
from utils.raster_utils.merge_rasters_gdal import merge_rasters_gdal
from utils.vector_utils.combine_vector_data import combine_vector_data
from utils.api import api_post_request
from process_forcing import ForcingProcessor
from scenario_selection.scenario_selector import scenarioSelector
import pandas as pd
import json
import sys


sys.path.append(r"d:\VSCode\IBF-flash-flood-pipeline")

logger = logging.getLogger(__name__)


def write_forcing_dict_to_csv(forcing_dict, output_loc):
    ts_coll = []
    for k, v in forcing_dict.items():
        ts = v.copy()
        ts = ts.rename(columns={"precipitation": k})
        ts = ts.set_index("datetime")
        ts.index = pd.to_datetime(ts.index)
        ts_coll.append(ts)

    pd.concat(ts_coll, axis=1).to_csv(output_loc)


def determine_trigger_states(
    karonga_events: dict, rumphi_events: dict, blantyre_events: dict
):
    """Determine for the three regions whether they should be triggered or not (based on the exposure of >20 people

    Args:
        karonga_events (dict): dictionary with events for karonga with the TA code as key and event as value
        rumphi_events (dict): dictionary with events for karonga with the TA code as key and event as value
        blantyre_events (dict): dictionary with events for karonga with the TA code as key and event as value

    Returns:
        trigger states for all areas (e.g. True/False)
    """

    if karonga_events:
        karonga_triggered_list = []
        for key, value in karonga_events.items():
            if key in THRESHOLD_CORRECTION_VALUES:
                threshold_value = (
                    ALERT_THRESHOLD_VALUE + THRESHOLD_CORRECTION_VALUES.get(key)
                )
            else:
                threshold_value = ALERT_THRESHOLD_VALUE
            region_file = gpd.read_file(
                str(DATA_FOLDER / value / "region_statistics.gpkg")
            )
            affected_people = region_file[region_file["placeCode"] == key][
                "affected_people"
            ].values[0]

            if affected_people is not None:
                karonga_triggered_list.append(
                    region_file[region_file["placeCode"] == key][
                        "affected_people"
                    ].values[0]
                    > threshold_value
                )
            else:
                karonga_triggered_list.append(False)
        karonga_trigger = any(karonga_triggered_list)
    else:
        karonga_trigger = False

    if rumphi_events:
        rumphi_triggered_list = []
        for key, value in rumphi_events.items():
            if key in THRESHOLD_CORRECTION_VALUES:
                threshold_value = (
                    ALERT_THRESHOLD_VALUE + THRESHOLD_CORRECTION_VALUES.get(key)
                )
            else:
                threshold_value = ALERT_THRESHOLD_VALUE
            region_file = gpd.read_file(
                str(DATA_FOLDER / value / "region_statistics.gpkg")
            )
            affected_people = region_file[region_file["placeCode"] == key][
                "affected_people"
            ].values[0]
            if affected_people is not None:
                rumphi_triggered_list.append(
                    region_file[region_file["placeCode"] == key][
                        "affected_people"
                    ].values[0]
                    > threshold_value
                )
            else:
                rumphi_triggered_list.append(False)
        rumphi_trigger = any(rumphi_triggered_list)
    else:
        rumphi_trigger = False

    if blantyre_events:
        blantyre_triggered_list = []
        for key, value in blantyre_events.items():
            if key in THRESHOLD_CORRECTION_VALUES:
                threshold_value = (
                    ALERT_THRESHOLD_VALUE + THRESHOLD_CORRECTION_VALUES.get(key)
                )
            else:
                threshold_value = ALERT_THRESHOLD_VALUE

            region_file = gpd.read_file(
                str(DATA_FOLDER / value / "region_statistics.gpkg")
            )
            affected_people = region_file[region_file["placeCode"] == key][
                "affected_people"
            ].values[0]
            if affected_people is not None:
                blantyre_triggered_list.append(
                    region_file[region_file["placeCode"] == key][
                        "affected_people"
                    ].values[0]
                    > threshold_value
                )
            else:
                blantyre_triggered_list.append(False)
        blantyre_trigger = any(blantyre_triggered_list)
    else:
        blantyre_trigger = False

    return karonga_trigger, rumphi_trigger, blantyre_trigger


def combine_events_and_upload_to_ibf(
    ta_gdf,
    events,
    lead_time,
    districtname,
    date=datetime.datetime.now(),
    additional_raster_paths=[],
    skip_depth_upload=False,
):
    """Combine the different conditions per TA for each province (e.g., combine a 90 mm in 12 hr scenario in rumphi boma with
    a 100 mm in 12 hr scenario in Chisowoko and upload the depth rasters, exposure status and TA values to the IBF portal

    Args:
        ta_gdf (gpd.GeoDataFrame): Dataframe with all TA's concerned by the pipeline.
        events (Dict): Dictionary with all events in the area considered (format: {"MW10203":"30mm2hr"})
        lead_time (int): leadtime for the region (e.g., 3 hours from now)
        districtname (str): name of the district for trigger warning name (e.g., Karonga, Rumphi)
        date (datetime.datetime): reference time used to upload to the IBF system
        additional_raster_paths (list): list of raster paths which belong to a different district but have not yet been uploaded.
            needed if multiple regions have the same leadtime.
        skip_depth_upload (Bool): whether depth upload should be skipped (in case of 2 regions with same leadtime). If Skip is provided a list
            of raster paths is returned to be uploaded the next time this function is called.

    Returns:
        data_uploader (DataUploader): class with all data to be uploaded
        raster_paths (list): list of all raster paths to be uploaded in the next function call.
    """
    logger.info(
        "step 3a started for vector data: clip and stitch data from one scenario per ta to one file for all tas together"
    )
    vector_datasets = {}

    event_ta_gdf = ta_gdf.loc[ta_gdf["placeCode"].isin(list(events.keys()))].copy()

    for asset_type in ASSET_TYPES:
        for key, value in events.items():
            event_ta_gdf.loc[event_ta_gdf["placeCode"] == key, "scenario"] = value

        vector_datasets[asset_type] = combine_vector_data(
            event_ta_gdf, DATA_FOLDER, asset_type
        )
    # for key, val in vector_datasets.items():

    #     if key != "region_statistics":
    #         val["id"] = pd.to_numeric(val["id"])

    #     val.to_file(
    #         Path(r"d:\VSCode\IBF-flash-flood-pipeline\data") / f"{key}_mock_event.gpkg"
    #     )

    logger.info(
        "step 3a finished for vector data: clip and stitch data from one scenario per ta to one file for all tas together"
    )

    # step (3b) - raster data: clip and stitch data of rasters (flood extent, affected people)
    logger.info(
        "step 3b started for raster data: clip and stitch data from one scenario per ta to one file for all tas together"
    )
    raster_paths = clip_rasters_on_ta(
        event_ta_gdf, DATA_FOLDER, Path(f"data/{ENVIRONMENT}/temp_rasters")
    )

    if not skip_depth_upload:
        raster_paths += [rf"data/static_data/{ENVIRONMENT}/nodata_ibf.tif"]
        raster_paths += additional_raster_paths
        merge_rasters_gdal(
            f"data/{ENVIRONMENT}/flood_extents/flood_extent_"
            + str(lead_time)
            + "-hour_MWI.tif",
            raster_paths,
        )
    logger.info(
        "step 3b finished for raster data: clip and stitch data from one scenario per ta to one file for all tas together"
    )

    # step (4): upload data and trigger
    logger.info(
        "step 4 started: upload and trigger tas, expose point assets, expose geoserver assets, upload raster file waterdepth"
    )
    data_uploader = DataUploader(
        time=str(lead_time) + "-hour",
        regions=vector_datasets["region_statistics"],
        district_name=districtname,
        schools=vector_datasets["vulnerable_schools"],
        waterpoints=vector_datasets["vulnerable_waterpoints"],
        roads=vector_datasets["vulnerable_roads"],
        buildings=vector_datasets["vulnerable_buildings"],
        health_sites=vector_datasets["vulnerable_health_sites"],
        date=date,
    )

    data_uploader.upload_and_trigger_tas()
    data_uploader.expose_point_assets()
    data_uploader.expose_geoserver_assets()

    if not skip_depth_upload:
        raster_uploader = RasterUploader(
            raster_files=[
                f"data/{ENVIRONMENT}/flood_extents/flood_extent_{str(lead_time)}-hour_MWI.tif"
            ]
        )
        raster_uploader.upload_raster_file()

    logger.info(
        "step 4 finished: upload and trigger tas, expose point assets, expose geoserver assets, upload raster file waterdepth"
    )
    if not skip_depth_upload:
        raster_paths = []
    return data_uploader, raster_paths


def historic_event_management(
    karonga_leadtime,
    karonga_trigger,
    karonga_events,
    rumphi_leadtime,
    rumphi_trigger,
    rumphi_events,
    blantyre_leadtime,
    blantyre_trigger,
    blantyre_events,
):

    leadtime_0_library_path = Path(rf"data/{ENVIRONMENT}/events/leadtime_0_events.json")

    if leadtime_0_library_path.exists():
        with open(
            leadtime_0_library_path,
            "r",
        ) as event_file:
            leadtime_0_dict = json.load(event_file)
    else:
        leadtime_0_dict = {}

    write_date = datetime.datetime.now().strftime("%d-%m-%Y_%H_%M")

    new_leadtime0_dict = {}

    if karonga_trigger and karonga_leadtime == 0:
        new_leadtime0_dict["karonga"] = karonga_events
    if rumphi_trigger and rumphi_leadtime == 0:
        new_leadtime0_dict["rumphi"] = rumphi_events
    if blantyre_trigger and blantyre_leadtime == 0:
        new_leadtime0_dict["blantyre"] = blantyre_events

    leadtime_0_dict[write_date] = new_leadtime0_dict

    if leadtime_0_dict:
        with open(
            leadtime_0_library_path,
            "w",
        ) as outfile:
            json.dump(leadtime_0_dict, outfile)

    with open(leadtime_0_library_path, "r") as event_file:
        historic_event = json.load(event_file)
        historic_event_dataframe = pd.DataFrame.from_dict(historic_event).T

        historic_event_dataframe.index = pd.to_datetime(
            historic_event_dataframe.index, format="%d-%m-%Y_%H_%M"
        )
        historic_event_dataframe = historic_event_dataframe.sort_index(ascending=False)

        # filter on max 5 days in past
        recent_historic_event_dataframe = historic_event_dataframe.loc[
            historic_event_dataframe.index
            > datetime.datetime.now() - datetime.timedelta(days=5)
        ]

        if "karonga" in recent_historic_event_dataframe.columns and not all(
            [
                pd.isnull(event)
                for event in recent_historic_event_dataframe["karonga"].tolist()
            ]
        ):
            karonga_leadtime = 0
            karonga_trigger = True
            karonga_events = recent_historic_event_dataframe.loc[
                recent_historic_event_dataframe[["karonga"]].first_valid_index(),
                "karonga",
            ]
        if "rumphi" in recent_historic_event_dataframe.columns and not all(
            [
                pd.isnull(event)
                for event in recent_historic_event_dataframe["rumphi"].tolist()
            ]
        ):
            rumphi_leadtime = 0
            rumphi_trigger = True
            rumphi_events = recent_historic_event_dataframe.loc[
                recent_historic_event_dataframe[["rumphi"]].first_valid_index(),
                "rumphi",
            ]
        if "blantyre" in recent_historic_event_dataframe.columns and not all(
            [
                pd.isnull(event)
                for event in recent_historic_event_dataframe["blantyre"].tolist()
            ]
        ):
            blantyre_leadtime = 0
            blantyre_trigger = True
            blantyre_events = recent_historic_event_dataframe.loc[
                recent_historic_event_dataframe[["blantyre"]].first_valid_index(),
                "blantyre",
            ]

    return (
        karonga_leadtime,
        karonga_trigger,
        karonga_events,
        rumphi_leadtime,
        rumphi_trigger,
        rumphi_events,
        blantyre_leadtime,
        blantyre_trigger,
        blantyre_events,
    )


def main():
    """Run impact based forecasting pipeline for malawi early warning system."""
    configure_logger()

    startTime = time.time()
    logger.info(f"IBF-Pipeline start: {str(datetime.datetime.now())}")

    # step (1): get gfs data per ta
    ta_gdf = gpd.read_file(rf"data/static_data/{ENVIRONMENT}/regions.gpkg")
    ta_gdf = ta_gdf.to_crs(epsg=4326)  # ,allow_override=True)

    logger.info("Step 1a: Retrieving forcing data")

    fp = ForcingProcessor(ta_gdf=ta_gdf)
    forcing_timeseries = fp.construct_forcing_timeseries()

    logger.info("Step 1b: Retrieving satellite data")
    gather_satellite_data()

    logger.info("Step 1c: Retrieving waterlevel sensor data")
    (
        gauges_actual_data_dict,
        gauges_reference_value_dict,
        gauges_yesterday_dict,
    ) = process_waterlevel_sensor_data()

    forcing_start_date = list(forcing_timeseries.values())[0].loc[0, "datetime"]

    karonga_rainfall_sensor_data = process_karonga_rainfall_sensor_data(
        start_date=forcing_start_date
    )

    write_forcing_dict_to_csv(
        forcing_dict=forcing_timeseries,
        output_loc=rf"data/{ENVIRONMENT}/debug_output/forcing_ts_before_gauge_{datetime.datetime.now().strftime('%Y-%m-%d-%H')}.csv",
    )

    if karonga_rainfall_sensor_data is not None:
        logger.info(
            "Step 1c.1: Overwriting satellite forcing with Karonga rainfall sensor data"
        )
        start_raingauge = karonga_rainfall_sensor_data.iloc[1].datetime

        end_raingauge = karonga_rainfall_sensor_data.iloc[-1].datetime

        df = forcing_timeseries["MW10407"].drop(
            forcing_timeseries["MW10407"]
            .loc[
                (forcing_timeseries["MW10407"]["datetime"] > start_raingauge)
                & (forcing_timeseries["MW10407"]["datetime"] <= end_raingauge)
            ]
            .index
        )
        df_combined = pd.concat(
            [karonga_rainfall_sensor_data, df], axis=0, ignore_index=True
        )
        df_combined.drop_duplicates(subset=["datetime"], inplace=True, keep="first")

        df_combined.sort_values(by=["datetime"], inplace=True)
        forcing_timeseries["MW10407"] = df_combined

    blantyre_rainfall_sensor_data = process_blantyre_rainfall_sensor_data()

    blantyre_raingauge_data_idw = blantyre_raingauge_idw(
        ta_gdf=ta_gdf, sensor_data_df=blantyre_rainfall_sensor_data
    )

    if len(blantyre_raingauge_data_idw) > 0:
        for ta in blantyre_raingauge_data_idw.columns:
            start_raingauge = blantyre_raingauge_data_idw.index[0]
            end_raingauge = blantyre_raingauge_data_idw.index[-1]

            gauge_data = blantyre_raingauge_data_idw[[ta]].copy()
            gauge_data = gauge_data.reset_index(names="datetime").rename(
                columns={ta: "precipitation"}
            )

            df = forcing_timeseries[ta].drop(
                forcing_timeseries[ta]
                .loc[
                    (forcing_timeseries[ta]["datetime"] > start_raingauge)
                    & (forcing_timeseries[ta]["datetime"] <= end_raingauge)
                ]
                .index
            )
            df_combined = pd.concat([gauge_data, df], axis=0, ignore_index=True)
            df_combined = df_combined.drop_duplicates(subset=["datetime"], keep="first")

            df_combined = df_combined.sort_values(by=["datetime"])
            forcing_timeseries[ta] = df_combined

    write_forcing_dict_to_csv(
        forcing_dict=forcing_timeseries,
        output_loc=rf"data/{ENVIRONMENT}/debug_output/forcing_ts_after_gauge_{datetime.datetime.now().strftime('%Y-%m-%d-%H')}.csv",
    )

    blantyre_rainfall_sensor_data.to_csv(
        rf"data/{ENVIRONMENT}/debug_output/blantyre_sensors_ts_{datetime.datetime.now().strftime('%Y-%m-%d_%H')}.csv"
    )

    # step (2): scenarioselector: choose scenario per ta
    logger.info("Step 2: Scenario selection")

    scenarios_selector = scenarioSelector(gfs_data=forcing_timeseries)
    (
        karonga_leadtime,
        karonga_events,
        rumphi_leadtime,
        rumphi_events,
        blantyre_leadtime,
        blantyre_events,
    ) = scenarios_selector.select_scenarios()

    logger.info("step 2 finished: scenario selection")
    # logger.info(str(datetime.datetime.now()))

    karonga_trigger, rumphi_trigger, blantyre_trigger = determine_trigger_states(
        karonga_events=karonga_events,
        rumphi_events=rumphi_events,
        blantyre_events=blantyre_events,
    )

    logger.info(f"karonga_leadtime before: {karonga_leadtime}")
    logger.info(f"karonga_trigger before: {karonga_trigger}")
    logger.info(f"karonga_events before: {karonga_events}")

    logger.info(f"rumphi_leadtime before: {rumphi_leadtime}")
    logger.info(f"rumphi_trigger before: {rumphi_trigger}")
    logger.info(f"rumphi_events before: {rumphi_events}")

    logger.info(f"blantyre_leadtime: {blantyre_leadtime}")
    logger.info(f"blantyre_trigger: {blantyre_trigger}")
    logger.info(f"blantyre_events: {blantyre_events}")

    (
        karonga_leadtime,
        karonga_trigger,
        karonga_events,
        rumphi_leadtime,
        rumphi_trigger,
        rumphi_events,
        blantyre_leadtime,
        blantyre_trigger,
        blantyre_events,
    ) = historic_event_management(
        karonga_leadtime,
        karonga_trigger,
        karonga_events,
        rumphi_leadtime,
        rumphi_trigger,
        rumphi_events,
        blantyre_leadtime,
        blantyre_trigger,
        blantyre_events,
    )

    logger.info(f"karonga_leadtime after: {karonga_leadtime}")
    logger.info(f"karonga_trigger after: {karonga_trigger}")
    logger.info(f"karonga_events after: {karonga_events}")

    logger.info(f"rumphi_leadtime after: {rumphi_leadtime}")
    logger.info(f"rumphi_trigger after: {rumphi_trigger}")
    logger.info(f"rumphi_events after: {rumphi_events}")

    logger.info(f"blantyre_leadtime after: {blantyre_leadtime}")
    logger.info(f"blantyre_trigger after: {blantyre_trigger}")
    logger.info(f"blantyre_events after: {blantyre_events}")

    region_trigger_metadata = pd.DataFrame(
        data={
            "region": ["Karonga", "Rumphi", "Blantyre"],
            "region_triggered": [karonga_trigger, rumphi_trigger, blantyre_trigger],
            "lead_time": [karonga_leadtime, rumphi_leadtime, blantyre_leadtime],
            "events": [karonga_events, rumphi_events, blantyre_events],
        }
    )

    triggered_regions = region_trigger_metadata.loc[
        region_trigger_metadata["region_triggered"]
    ]
    triggered_regions = triggered_regions.sort_values(by="lead_time", ascending=True)

    date = datetime.datetime.now(
        tz=datetime.timezone.utc
    )  # check if it is ok that the current date is generated twice, even though it is not used in the shape post request

    for lead_time in np.unique(triggered_regions["lead_time"].tolist()):
        triggered_regions_leadtime_filter = triggered_regions.loc[
            triggered_regions["lead_time"] == lead_time
        ]

        if len(triggered_regions_leadtime_filter) > 1:
            raster_path_collection = []

            for i, (_, row) in enumerate(triggered_regions_leadtime_filter.iterrows()):
                if i < len(triggered_regions_leadtime_filter) - 1:
                    skip_depth_map_upload = True
                else:
                    skip_depth_map_upload = False

                _, additional_raster_paths = combine_events_and_upload_to_ibf(
                    ta_gdf=ta_gdf,
                    events=row["events"],
                    lead_time=int(row["lead_time"]),
                    districtname=row["region"],
                    date=date,
                    additional_raster_paths=raster_path_collection,
                    skip_depth_upload=skip_depth_map_upload,
                )
                raster_path_collection.extend(additional_raster_paths)
        else:
            combine_events_and_upload_to_ibf(
                ta_gdf=ta_gdf,
                events=triggered_regions_leadtime_filter["events"].iloc[0],
                lead_time=int(triggered_regions_leadtime_filter["lead_time"].iloc[0]),
                districtname=triggered_regions_leadtime_filter["region"].iloc[0],
                date=date,
            )

    # step (3a) - vector data: clip and stitch data of assets
    # date = datetime.datetime.now()

    # upload gauge data
    gauge_data_uploader = DataUploader(
        time=None,
        regions=ta_gdf,
        district_name=None,
        schools=None,
        waterpoints=None,
        roads=None,
        buildings=None,
        health_sites=None,
        sensor_actual_values_dict=gauges_actual_data_dict,
        sensor_previous_values_dict=gauges_yesterday_dict,
        sensor_reference_values_dict=gauges_reference_value_dict,
        date=date,
    )
    gauge_data_uploader.upload_sensor_values()

    if not karonga_trigger and not rumphi_trigger and not blantyre_trigger:
        portal_resetter = DataUploader(
            time=None,
            regions=ta_gdf,
            district_name=None,
            schools=None,
            waterpoints=None,
            roads=None,
            buildings=None,
            health_sites=None,
            date=date,
        )
        portal_resetter.untrigger_portal()
        logger.info("Untriggered portal")

    logger.info("Closing Events...")

    api_post_request(
        "event/close-events",
        body={
            "countryCodeISO3": "MWI",
            "disasterType": "flash-floods",
            "date": date.strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
    )
    gauge_data_uploader.send_notifications()
    elapsedTime = str(time.time() - startTime)
    logger.info(str(elapsedTime))


if __name__ == "__main__":
    main()
