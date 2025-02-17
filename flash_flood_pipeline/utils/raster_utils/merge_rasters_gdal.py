from osgeo import gdal


def merge_rasters_gdal(output_path_raster, raster_paths):
    """
    Merge rasters for individual TA's to one combined raster

    Args:
        output_path_raster (str): path where the raster should be saved to (should have IBF compliant filename)
        raster_paths (list): list of the individual rasters which should be merged to get to the combined raster
    """
    # Open the input rasters
    # raster_paths+=["malawi_nodata.tif"]
    gdal.BuildVRT("merged.vrt", raster_paths)

    original_pixsize = 1.358452567584323166e-05

    translate_options = gdal.TranslateOptions(
        format="GTiff",
        creationOptions=["COMPRESS=DEFLATE"],
        xRes=original_pixsize * 2,
        yRes=original_pixsize * 2,
    )
    gdal.Translate(output_path_raster, "merged.vrt", options=translate_options)
