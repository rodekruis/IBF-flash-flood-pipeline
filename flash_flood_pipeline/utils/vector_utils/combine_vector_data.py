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
        asset_type (str): Type of asset to be read and combined. Choose from: 'vulnerable_buildings', 'region_statistics', 'vulnerable_health_sites', 'vulnerable_roads',
        'vulnerable_schools', 'vulnerable_waterpoints'

    Returns:
        merged_vector_layer (pd.DataFrame): dataset with assets and their exposure for the different TA's
    """

    ta_gdf_3857 = ta_df.copy().to_crs(3857)

    if asset_type == "region_statistics":
        region_statistics_template = gpd.read_file(
            rf"data/static_data/{ENVIRONMENT}/region_statistics_zeroes.gpkg"
        )
        region_statistics_template = region_statistics_template.rename(
            columns={"ADM3_PCODE": "placeCode"}
        )
        region_statistics_untriggered_tas = region_statistics_template.loc[
            ~region_statistics_template["placeCode"].isin(
                ta_gdf_3857["placeCode"].tolist()
            )
        ]

        region_statistics_collection = [region_statistics_untriggered_tas]

        for _, row in ta_gdf_3857.iterrows():
            vector_layer_of_interest = gpd.read_file(
                str(data_folder / row["scenario"] / asset_type) + ".gpkg"
            )

            vector_layer_of_interest = vector_layer_of_interest.fillna(0)
            vector_layer_of_interest_subset = vector_layer_of_interest[
                vector_layer_of_interest["placeCode"] == row["placeCode"]
            ]
            region_statistics_collection.append(vector_layer_of_interest_subset)

        merged_vector_layer = pd.concat(region_statistics_collection, axis=0)

    else:
        vector_layers = []

        for _, row in ta_gdf_3857.iterrows():
            if row["scenario"] != "":
                vector_layer_of_interest = gpd.read_file(
                    str(data_folder / row["scenario"] / asset_type) + ".gpkg",
                    mask=row["geometry"],
                )
                features_within_ta = gpd.clip(vector_layer_of_interest, row["geometry"])

                if len(features_within_ta) > 0:
                    features_within_ta_filtered = features_within_ta[
                        ["id", "vulnerability"]
                    ]
                    vector_layers.append(features_within_ta_filtered)

        merged_vector_layer = pd.concat(vector_layers)

    return merged_vector_layer
