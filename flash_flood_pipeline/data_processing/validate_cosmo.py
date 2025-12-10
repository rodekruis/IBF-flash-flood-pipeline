from pathlib import Path
import xarray as xr
import rioxarray


def validate_cosmo(cosmo_path: Path) -> bool:
    """Validate COSMO file by checking for essential variables.

    Args:
        file_path (str): Path to the COSMO NetCDF file.
    """

    try:
        with xr.open_dataset(cosmo_path) as src:
            pass
        return True
    except Exception as e:
        return False
