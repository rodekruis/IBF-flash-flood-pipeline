import numpy as np


def extract_lat_lon(ds):
    """
    Create 2D arrays with latitudes and longitudes from netCDF dataset variables.
    ds - netCDF dataset

    Returns latvar, lonvar
    """
    latvar = ds.variables["lat"][:]
    lat_dim = len(latvar)

    lonvar = ds.variables["lon"][:]
    lon_dim = len(lonvar)

    latvar = np.stack([latvar for _ in range(lon_dim)], axis=0)
    lonvar = np.stack([lonvar for _ in range(lat_dim)], axis=1)
    return latvar, lonvar
