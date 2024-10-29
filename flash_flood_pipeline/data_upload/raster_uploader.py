import logging
from utils.api import api_post_request
from settings.base import DISASTER_TYPE

logger = logging.getLogger(__name__)


class RasterUploader:
    """
    Class to upload raster file to ibf API.
    """

    def __init__(self, raster_files):
        self.raster_files = raster_files

    def upload_raster_file(self):
        """
        Upload raster_files (class attribute) to ibf raster endpoint
        """
        for raster_file in self.raster_files:
            files = {"file": open(raster_file, "rb")}
            api_post_request(
                "admin-area-dynamic-data/raster/" + DISASTER_TYPE, files=files
            )
            logger.info(f"Uploaded raster-file: {raster_file}")
            # Path(raster_file).unlink()
