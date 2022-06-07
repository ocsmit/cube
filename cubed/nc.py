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

from cubed.date_glob import DateGlob


def dir_to_xarr(
    file_dir: Path,
    date_expression: str = "%Y-%m-%d",
    file_ext: str = "tif",
) -> xr.DataArray:

    globexp = DateGlob(date_expression)
    files = list(file_dir.rglob(f"*{globexp.pattern}*.{file_ext}"))
    if len(files) == 0:
        raise FileNotFoundError(
            f"No files found in '{str(file_dir)}' with date '{date_expression}'"
        )

    regexp = fr"{globexp.pattern}*|$"

    dates = [
        datetime.strptime(re.search(regexp, str(f)).group(), date_expression)
        for f in files
    ]
    # Standard xarray convention seems to name all time variables as time, even just dates
    time = xr.Variable("time", dates)
    xr_arr = xr.concat(
        [rioxarray.open_rasterio(f, chunks=True) for f in files], dim=time
    )
    # xr_arr = xr_arr.assign_coords(band=band_names)
    return xr_arr.sortby("time")
