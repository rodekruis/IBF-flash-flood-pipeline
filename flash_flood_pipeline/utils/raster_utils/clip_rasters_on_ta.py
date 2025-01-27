import rasterio
from pathlib import PurePosixPath as Path


def clip_rasters_on_ta(ta_df, data_folder, output_folder):
    """
    Clip the country wide flood map to a flood map per TA for each TA where a flood event is predicted.

    Args:
        ta_df (gpd.GeoDataFrame): dataframe with all TA's. Contains a scenario column which describes the forecasted flood event for that TA
        data_folder (Path): Path to input data folder where the different scenario datasets are stored
        output_folder (Path): Folder where the clipped rasters should be stored

    returns:
        raster_path (list): list of paths to the raster files clipped/created
    """
    Path(output_folder).mkdir(exist_ok=True)
    raster_paths = []
    for _, row in ta_df.iterrows():
        if row["scenario"] != "":
            input_raster = data_folder / row["scenario"] / "depth.tif"
            with rasterio.open(input_raster) as src:
                out_image, out_transform = rasterio.mask.mask(
                    src, [row["geometry"]], crop=True
                )
                out_meta = src.meta

            out_meta.update(
                {
                    "driver": "GTiff",
                    "height": out_image.shape[1],
                    "width": out_image.shape[2],
                    "transform": out_transform,
                    "compress": "deflate",
                }
            )

            raster_path = str(output_folder / row["placeCode"]) + ".tif"
            raster_paths.append(raster_path)
            with rasterio.open(raster_path, "w", **out_meta) as dest:
                dest.write(out_image)
            del out_image
    return raster_paths
