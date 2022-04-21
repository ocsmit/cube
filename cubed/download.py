#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
    cube.download
    ~~~~~~~~~~~~~~~~~~~

    Methods for downloading from NASA Earthdata and generating tifs, netcdf,
    and xarray data cubes.

    :copyright: (c) 2022 by Owen Smith.
    :license: GNU v3.0, see LICENSE for more details.
"""


# Standard library imports
import json
from pathlib import Path
from datetime import datetime
import multiprocessing as mp
from typing import Dict, Any, Union
import time

# Third party imports
import pandas as pd
import geopandas as gpd
from osgeo import gdal
import xarray as xr
import rioxarray
from shapely.geometry import mapping
from cubed.utils import parse_bandmap
from cubed.client import generate_client

try:
    from rich import print
except ImportError:
    pass

rioxarray.set_options(export_grid_mapping=False)
# Local imports
# ...

######################################
# Types
ItemCollectionDict = Dict[str, Any]

######################################
# Set up VSIcurl settings
gdal.SetConfigOption("GDAL_HTTP_COOKIEFILE", "~/cookies.txt")
gdal.SetConfigOption("GDAL_HTTP_COOKIEJAR", "~/cookies.txt")
gdal.SetConfigOption("GDAL_DISABLE_READDIR_ON_OPEN", "YES")
gdal.SetConfigOption("CPL_VSIL_CURL_ALLOWED_EXTENSIONS", "TIF")
######################################

#####################################
# Consts
PROCESSES = 8

CATALOGS = ["LPCLOUD"]


def get_links(asset, band_map: Dict[str, str]) -> pd.DataFrame:
    """Generate data frame of hrefs and meta data for a scene

    Parameters
    ----------
    asset : ?
        Single asset catalog of band items
    band_map: Dict[str, str]
        Band map look up

    Return
    -------
    pandas.DataFrame
        Pandas object which contains dates, collection, band, and href link

    Notes
    -----

    Example
    -------
    """
    data_dict = []
    sat = asset.collection_id
    band_keys = list(band_map.get(sat).keys())
    date = asset.datetime.date()
    for idx, item in enumerate(asset.assets):
        if item in band_keys:
            cname = band_map.get(sat).get(item)
            href = asset.assets.get(item).href
            data_dict.append(
                {
                    "date": date,
                    "sat": sat,
                    "band": item,
                    "cname": cname,
                    "href": href,
                }
            )
    return pd.DataFrame(data_dict)


def construct_file_df(
    files: ItemCollectionDict, band_map: Dict[str, str]
) -> Union[pd.DataFrame, None]:
    """Construct dataframe for entire query

    Parameters
    ----------
    files : ItemCollectionDict
        ItemCollection search result
    band_map: Dict[str, str]
        Band map look up


    Returns
    -------
    pandas.DataFrame
        Pandas object which contains dates, collection, band, and href link
        for each item in ItemCollection

    Notes
    -----
        Different from get_links() in that this generates data frame entry
        for each asset in each item in the ItemCollection

    Example
    -------
    """
    file_list = []
    for i in files:
        file_list.append(get_links(i, band_map))
    href_df = pd.concat(file_list)
    return href_df.sort_values(by=["date", "band"])


def generate_cube(
    hrefs: pd.DataFrame,
    geom: gpd.GeoDataFrame,
    processes: int = PROCESSES,
    cache=True,
    cache_dir: Union[Path, str] = Path("./"),
) -> xr.core.dataarray.DataArray:
    """Create cube from ItemCollection

    Parameters
    ----------
    hrefs : pandas.DataFrame
        Pandas object which contains dates, collection, band, and href link
        for each item in ItemCollection
    geom : geopandas.GeoDataFrame
        Geometry object to clip imagery to
    processes : int
        Number of CPU cores to utilize
    cache : bool
        Whether or not to cache all imgs as GeoTiffs
    cache_dir : Path
        Directory in which to output downloaded imagery if cache is true


    Returns
    -------
    xarray.DataArray
        xarray DataArray object with dimensions [dates, col, row, band]

    Notes
    -----

    Example
    -------
    """
    dates = hrefs["date"].unique()
    time = xr.Variable("time", dates)
    # pool = mp.Pool(processes)
    # imgs = pool.starmap(
    #    band_stack,
    #    [(hrefs.loc[hrefs["date"] == d], geom, cache, cache_dir) for d in dates],
    # )

    imgs = [band_stack(hrefs.loc[hrefs["date"] == d], geom, False) for d in dates]

    return imgs


def band_stack(
    href_df: pd.DataFrame,
    geoms: gpd.GeoDataFrame,
    cache=True,
    cache_dir: Path = Path("./"),
) -> Union[xr.Dataset, None]:
    """Method to create single date data cube object

    Parameters
    ----------
    href_df : pandas.DataFrame
        Pandas object which contains dates, collection, band, and href link
    geom : geopandas.GeoDataFrame
        Geometry object to clip imagery to
    cache : bool
        Whether or not to cache all imgs as GeoTiffs
    cache_dir : Path
        Directory in which to output downloaded imagery if cache is true


    Returns
    -------
    xarray.DataArray
        xarray DataArray object with dimensions [date, col, row, band]

    Notes
    -----

    Example
    -------
    """
    s = time.time()
    bands = list(href_df.cname)
    band = xr.Variable("band", bands)
    sat = href_df["sat"].unique()[0]
    date = href_df["date"].unique()[0]
    print(f"{datetime.now()} {sat} {date}")
    xr_arr = xr.concat(
        [
            rioxarray.open_rasterio(f, chunks=True,).rio.clip(
                geoms.geometry.apply(mapping),
                geoms.crs,
                from_disk=True,
            )
            for f in href_df.href
        ],
        dim=band,
    )
    xr_arr.attrs["long_name"] = bands
    sat = href_df["sat"].unique()[0]
    return xr_arr


if __name__ == "__main__":
    band_map = parse_bandmap("HLSv2")
    catalog = generate_client()
    with open("../test/data/test_poly.geojson", "r") as fp:
        region_model = json.load(fp)
        geom = region_model["features"][0]["geometry"]

    search = catalog.search(collections=list(band_map.keys()), intersects=geom)

    item_collection = search.get_all_items()
    files = list(item_collection)
    hrefs = construct_file_df(files, band_map)
    geom = gpd.read_file("../test/data/test_poly.geojson")

    test5 = generate_cube(hrefs, geom, cache_dir=Path("/SEAL/OwenSmith/hls_cube"))
