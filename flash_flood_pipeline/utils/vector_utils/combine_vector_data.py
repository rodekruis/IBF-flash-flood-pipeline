import geopandas as gpd
import pandas as pd
from settings.base import ENVIRONMENT


def combine_vector_data(ta_df, data_folder, asset_type):
    """
        Function to read the exposure data for each TA for its flooding scenario. For example:
        Reading the 100mm in 24 hours datasets for Karonga and the 20mm in 12 hours datasets for rumphi
        and combining these into 1 dataframe which can be used for posting to the portal.

    Args:
        ta_df (gpd.GeoDataFrame): dataframe with TAs and their corresponding flood scenario
        data_folder (Path): Path to folder where all input data is stored
        asset_type (str): Type of asset to be read and combined. Choose from: 'vulnerable_buildings', 'region_statistics', 'vulnerable_health_sites', 'vulnerable_roads', 'vulnerable_schools', 'vulnerable_waterpoints'

    Returns:
        merged_vector_layer (pd.DataFrame): dataset with assets and their exposure for the different TA's
    """
    vector_layers = []
    ta_gdf_3857 = ta_df.copy().to_crs(3857)

    for _, row in ta_gdf_3857.iterrows():
        if row["scenario"] != "":
            if asset_type != "region_statistics":
                vector_layer_of_interest = gpd.read_file(
                    str(data_folder / row["scenario"] / asset_type) + ".gpkg",
                    mask=row["geometry"],
                )
                vector_layers.append(
                    gpd.clip(vector_layer_of_interest, row["geometry"])
                )
            else:
                vector_layer_of_interest = gpd.read_file(
                    str(data_folder / row["scenario"] / asset_type) + ".gpkg"
                )

                vector_layer_of_interest = vector_layer_of_interest.fillna(0)
                vector_layer_of_interest_subset = vector_layer_of_interest[
                    vector_layer_of_interest["placeCode"] == row["placeCode"]
                ]

                vector_layers.append(vector_layer_of_interest_subset)
        if asset_type == "region_statistics":
            region_statistics_template = gpd.read_file(
                rf"data/static_data/{ENVIRONMENT}/region_statistics_zeroes.gpkg"
            )
            region_statistics_template = region_statistics_template.rename(
                columns={"ADM3_PCODE": "placeCode"}
            )
            vector_layers.append(region_statistics_template)

    merged_vector_layer = pd.concat(vector_layers)
    if asset_type == "region_statistics":
        merged_vector_layer = merged_vector_layer.drop_duplicates(
            subset="placeCode", keep="first"
        )

    return merged_vector_layer
