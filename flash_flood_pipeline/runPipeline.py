import datetime
import time
from pathlib import Path
import geopandas as gpd
import numpy as np
import logging
from settings.base import DATA_FOLDER, ASSET_TYPES, ENVIRONMENT
from logger_config.configure_logger import configure_logger
from data_download.collect_data import dataGetter
from data_upload.upload_results import DataUploader
from data_upload.raster_uploader import RasterUploader
from utils.raster_utils.clip_rasters_on_ta import clip_rasters_on_ta
from utils.raster_utils.merge_rasters_gdal import merge_rasters_gdal
from utils.vector_utils.combine_vector_data import combine_vector_data

from scenario_selector import scenarioSelector
import pandas as pd

import sys

sys.path.append(r"d:\VSCode\IBF_FLASH_FLOOD_PIPELINE")

logger = logging.getLogger(__name__)


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
                    > 20
                )
            else:
                karonga_triggered_list.append(False)
        karonga_trigger = any(karonga_triggered_list)
    else:
        karonga_trigger = False

    if rumphi_events:
        rumphi_triggered_list = []
        for key, value in rumphi_events.items():
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
                    > 20
                )
            else:
                rumphi_triggered_list.append(False)
        rumphi_trigger = any(rumphi_triggered_list)
    else:
        rumphi_trigger = False

    if blantyre_events:
        blantyre_triggered_list = []
        for key, value in blantyre_events.items():
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
                    > 20
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

    ta_gdf = ta_gdf[ta_gdf["placeCode"].isin(list(events.keys()))]

    for asset_type in ASSET_TYPES:
        for key, value in events.items():
            ta_gdf.loc[ta_gdf["placeCode"] == key, "scenario"] = value

        vector_datasets[asset_type] = combine_vector_data(
            ta_gdf, DATA_FOLDER, asset_type
        )

    logger.info(
        "step 3a finished for vector data: clip and stitch data from one scenario per ta to one file for all tas together"
    )

    # step (3b) - raster data: clip and stitch data of rasters (flood extent, affected people)
    logger.info(
        "step 3b started for raster data: clip and stitch data from one scenario per ta to one file for all tas together"
    )
    raster_paths = clip_rasters_on_ta(ta_gdf, DATA_FOLDER, Path("data/temp_rasters"))

    if not skip_depth_upload:
        raster_paths += [rf"data/static_data/{ENVIRONMENT}/nodata_ibf.tif"]
        raster_paths += additional_raster_paths
        merge_rasters_gdal(
            "data/flood_extent_" + str(lead_time) + "-hour_MWI.tif", raster_paths
        )
    logger.info(
        "step 3b finished for raster data: clip and stitch data from one scenario per ta to one file for all tas together"
    )

    # step (4): upload data and trigger
    logger.info(
        "step 4 started: upload and trigger tas, expose point assets, expose geoserver assets, upload raster file waterdepth"
    )
    data_uploader = DataUploader(
        str(lead_time) + "-hour",
        vector_datasets["region_statistics"],
        districtname,
        vector_datasets["vulnerable_schools"],
        vector_datasets["vulnerable_waterpoints"],
        vector_datasets["vulnerable_roads"],
        vector_datasets["vulnerable_buildings"],
        vector_datasets["vulnerable_health_sites"],
        date,
    )
    data_uploader.upload_and_trigger_tas()
    data_uploader.expose_point_assets()
    data_uploader.expose_geoserver_assets()

    if not skip_depth_upload:
        raster_uploader = RasterUploader(
            raster_files=[f"data/flood_extent_{str(lead_time)}-hour_MWI.tif"]
        )
        raster_uploader.upload_raster_file()

    logger.info(
        "step 4 finished: upload and trigger tas, expose point assets, expose geoserver assets, upload raster file waterdepth"
    )
    if not skip_depth_upload:
        raster_paths = []
    return data_uploader, raster_paths


def main():
    """Run impact based forecasting pipeline for malawi early warning system."""
    configure_logger()

    startTime = time.time()
    logger.info(str(datetime.datetime.now()))

    # step (1): get gfs data per ta
    ta_gdf = gpd.read_file(rf"data/static_data/{ENVIRONMENT}/regions.gpkg")
    ta_gdf = ta_gdf.to_crs(epsg=4326)  # ,allow_override=True)
    logger.info("step 1 started: retrieving gfs data with API-request")
    data_getter = dataGetter(ta_gdf)
    gfs_data = data_getter.get_rain_forecast()
    data_getter.gather_satellite_data()

    (
        gauges_actual_data_dict,
        gauges_reference_value_dict,
        gauges_yesterday_dict,
    ) = data_getter.get_sensor_values()

    rain_sensor_data = data_getter.get_rain_gauge()

    if rain_sensor_data is not None:
        start_raingauge = rain_sensor_data.iloc[1].datetime

        end_raingauge = rain_sensor_data.iloc[-1].datetime
        df = gfs_data["MW10407"].drop(
            gfs_data["MW10407"]
            .loc[
                (gfs_data["MW10407"]["datetime"] > start_raingauge)
                & (gfs_data["MW10407"]["datetime"] <= end_raingauge)
            ]
            .index
        )
        df_combined = pd.concat([rain_sensor_data, df], axis=0, ignore_index=True)
        df_combined.drop_duplicates(subset=["datetime"], inplace=True, keep="first")

        df_combined.sort_values(by=["datetime"], inplace=True)
        gfs_data["MW10407"] = df_combined

    logger.info("step 1 finished: retrieving GFS/COSMO data with API-request")
    logger.info(str(datetime.datetime.now()))

    # step (2): scenarioselector: choose scenario per ta
    logger.info("step 2 started: scenario selection")
    scenarios_selector = scenarioSelector(gfs_data)
    (
        karonga_leadtime,
        karonga_events,
        rumphi_leadtime,
        rumphi_events,
        blantyre_leadtime,
        blantyre_events,
    ) = scenarios_selector.select_scenarios()

    # TODO: remove until next comment (testing)
    #karonga_leadtime = 1
    #karonga_events = {"MW10203": "100mm_12hr"}

    #blantyre_leadtime = 1
    #blantyre_events = {
    #    "MW31533": "100mm_12hr",
    #    "MW31534": "100mm_12hr",
    #    "MW31532": "100mm_12hr",
    #    "MW31541": "100mm_12hr",
    #}
    # end of testing segment

    logger.info("step 2 finished: scenario selection")
    logger.info(str(datetime.datetime.now()))

    karonga_trigger, rumphi_trigger, blantyre_trigger = determine_trigger_states(
        karonga_events=karonga_events,
        rumphi_events=rumphi_events,
        blantyre_events=blantyre_events,
    )

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

    date = datetime.datetime.now()

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
    date = datetime.datetime.now()

    # upload gauge data
    gauge_data_uploader = DataUploader(
        None,
        ta_gdf,
        None,
        None,
        None,
        None,
        None,
        date,
        sensor_actual_values_dict=gauges_actual_data_dict,
        sensor_previous_values_dict=gauges_yesterday_dict,
        sensor_reference_values_dict=gauges_reference_value_dict,
    )
    gauge_data_uploader.upload_sensor_values()

    if not karonga_trigger and not rumphi_trigger and not blantyre_trigger:
        portal_resetter = DataUploader(
            None, ta_gdf, None, None, None, None, None, None, date
        )
        portal_resetter.untrigger_portal()
        print("untrigger portal")
    else:
        print("would have notified")
        # data_uploader.send_notifications()

    elapsedTime = str(time.time() - startTime)
    logger.info(str(elapsedTime))


if __name__ == "__main__":
    main()
