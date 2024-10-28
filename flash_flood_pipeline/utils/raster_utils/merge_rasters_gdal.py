from osgeo import gdal


def merge_rasters_gdal(output_path_raster, raster_paths):
    """
    Merge rasters to one output raster for all TAS combined
    """
    # Open the input rasters
    # raster_paths+=["malawi_nodata.tif"]
    print(raster_paths)
    gdal.BuildVRT("merged.vrt", raster_paths)

    original_pixsize = 1.358452567584323166e-05

    translate_options = gdal.TranslateOptions(
        format="GTiff",
        creationOptions=["COMPRESS=DEFLATE"],
        xRes=original_pixsize * 2,
        yRes=original_pixsize * 2,
    )
    gdal.Translate(output_path_raster, "merged.vrt", options=translate_options)
