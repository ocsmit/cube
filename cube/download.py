#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
    cube.download
    ~~~~~~~~~~~~~

    Methods for downloading from NASA Earthdata and generating tifs, netcdf,
    and xarray data cubes.

    :copyright: (c) 2022 by Owen Smith.
    :license: GNU v3.0, see LICENSE for more details.
"""


# Standard library imports
import os
import json
from pathlib import Path
from datetime import datetime
import multiprocessing as mp
from typing import Dict, Any, Union

# Third party imports
import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from osgeo import gdal
import rasterio as rio
import xarray as xr
import rioxarray
from shapely.geometry import mapping
from pystac_client import Client, ItemSearch

try:
    from rich import print
except ImportError:
    pass

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
PROCESSES = 4

CATALOGS = ["LPCLOUD"]

HLSV2 = {
    "HLSL30.v2.0": {
        "B01": "aerosal",
        "B02": "blue",
        "B03": "green",
        "B04": "red",
        "B05": "nir",
        "B06": "swir1",
        "B07": "swir2",
        "B09": "cirrus",
        "Fmask": "fmask",
    },
    "HLSS30.v2.0": {
        "B01": "aerosal",
        "B02": "blue",
        "B03": "green",
        "B04": "red",
        "B08A": "nir",
        "B10": "cirrus",
        "B11": "swir1",
        "B12": "swir2",
        "Fmask": "fmask",
    },
}


def inv_bandmap(band_map: Dict[str, str]) -> Dict[str, str]:
    """Generate inverted band, common name dictonary

    Parameters
    ----------
    band_map : dict
        Band map for data product

    Returns
    -------
    dict
        Band map with inverted band name and common name key, value pair

    Example
    -------
        bmap =  {"HLSS30.v2.0": {
                "B01": "aerosal"}}
        inv_bandmap(bmap)

    """
    inv_dict = {}
    collections = list(band_map.keys())
    for c in collections:
        inv_dict[c] = {v: k for k, v in band_map.get(c).items()}
    return inv_dict


def generate_client(
    catalog="LPCLOUD", url="https://cmr.earthdata.nasa.gov/stac/"
) -> Client:
    """Connect to nasa stac

    Parameters
    ----------
    catalog : str
        Catalog to ingest from
    url : str
        STAC URL

    Returns
    -------
    Client
        STAC endpoint

    Notes
    -----
        Simply a wrapper for Client.open.
        TODO: Add checking and indexing of NASA cmr catalogs

    """
    return Client.open(f"{url}/{catalog}")


def get_links(asset, band_map: Dict[str, str]) -> pd.DataFrame:
    """Generate data frame of hrefs and meta data for a scene

    Parameters
    ----------
    asset : ?
        Single asset catalog of band items
    band_map: Dict[str, str]
        Band map look up

    Returns
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
) -> pd.DataFrame:
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
    dates = hrefs["date"].unique()
    time = xr.Variable("time", dates)
    pool = mp.Pool(processes)
    imgs = pool.starmap(
        band_stack,
        [(hrefs.loc[hrefs["date"] == d], geom, cache, cache_dir) for d in dates],
    )
    return imgs


def band_stack(
    href_df: pd.DataFrame,
    geoms: gpd.GeoDataFrame,
    cache=True,
    cache_dir: Union[Path, str] = Path("./"),
) -> xr.core.dataarray.DataArray:
    # dates_ind = sorted_enumerate(dates)
    # Standard xarray convention seems to name all time variables as time, even just dates
    bands = list(href_df.cname)
    band = xr.Variable("band", bands)
    sat = href_df["sat"].unique()[0]
    date = href_df["date"].unique()[0]
    print(f"{datetime.now()} {sat} {date}")
    xr_bands = [
        rioxarray.open_rasterio(f, masked=True).rio.clip(
            geoms.geometry.apply(mapping), geoms.crs
        )
        for f in href_df.href
    ]
    xr_arr = xr.concat(
        xr_bands,
        dim=band,
    )
    xr_arr.attrs["long_name"] = bands
    if cache is True:
        sat = href_df["sat"].unique()[0]
        out_path = cache_dir / f"{sat}_{date}.tif"
        xr_arr.rio.to_raster(out_path)
        return None
    else:
        return xr_arr


if __name__ == "__main__":
    catalog = generate_client()
    with open("../test/data/test_poly.geojson", "r") as fp:
        region_model = json.load(fp)
        geom = region_model["features"][0]["geometry"]

    search = catalog.search(collections=list(HLSV2.keys()), intersects=geom)

    item_collection = search.get_all_items()
    files = list(item_collection)
    hrefs = construct_file_df(files, HLSV2)
    geom = gpd.read_file("./test_poly.geojson")

    test5 = generate_cube(hrefs, geom, cache_dir=Path("/SEAL/OwenSmith/hls_cube"))
