#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
    cube.nc
    ~~~~~~~

    Methods for generating various netcdf files

    :copyright: (c) 2022 by Owen Smith.
    :license: GNU v3.0, see LICENSE for more details.
"""


import xarray as xr
import numpy as np
from pathlib import Path
import rioxarray
from datetime import datetime
import re
import matplotlib.pyplot as plt
import geopandas as gpd


def convert_to_xarr(
    file_dir: Path,
    date_expression: str = "",
    file_ext: str = "tif",
    band_names: list = [
        "blue",
        "green",
        "red",
        "nir08",
        "swir16",
        "swir22",
        "ndvi",
        "qa",
    ],
) -> xr.core.dataarray.DataArray:

    files = list(file_dir.rglob(f"*.{file_ext}"))
    dates = [datetime.strptime(i.stem.split(".")[-1], "%Y%m%d") for i in files]
    dates_ind = sorted_enumerate(dates)
    # Standard xarray convention seems to name all time variables as time, even just dates
    time = xr.Variable("time", dates)
    xr_arr = xr.concat(
        [rioxarray.open_rasterio(f, masked=True) for f in files], dim=time
    )
    xr_arr = xr_arr.assign_coords(band=band_names)
    return xr_arr.sortby("time")
