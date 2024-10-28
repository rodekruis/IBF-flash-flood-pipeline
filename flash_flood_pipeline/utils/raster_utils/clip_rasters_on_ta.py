import rasterio
from pathlib import Path


def clip_rasters_on_ta(ta_df, data_folder, output_folder):
    """
    Clip a raster with a TA shapefile and export it as a new raster output.

    Args:
        SCENARIO_TAS (dict): Dictionary in which for each placeCode of an TA the chosen scenario is defined. Example:
        {'MW10220': '60mm1hr_north', 'MW10203': '100mm48hr_north', 'MW10106': '60mm1hr_north'}.
        raster (str): string of the rastername to be clipped.
        output_folder (str) : string/path directory of the folder for the to be stored rasters
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
