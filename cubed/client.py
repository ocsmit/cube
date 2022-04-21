from pystac_client import Client
from pystac_client.item_search import ItemSearch
import geopandas as gpd
from typing import Union, Any, Dict, Tuple
from pathlib import Path
import json

from pystac_client.exceptions import APIError
from pystac_client import Client

import warnings

# Type
SingleGeom = Dict[str, Union[str, Tuple]]


class CubeClient(Client):
    """
    Extends the pystac_client Client class to provide handling of geometry
    passed as a geojson path or GeoPandas dataframe object
    """

    def __init__(self, **kwargs: Any) -> None:
        super(Client, self).__init__(**kwargs)

    def search(self, **kwargs: Any) -> ItemSearch:
        # Wrap parent ItemSearch with geometry check
        if "intersects" in list(kwargs.keys()):
            geom = kwargs.get("intersects")
            kwargs["intersects"] = self.__check_intersects(geom)
        return super(CubeClient, self).search(**kwargs)

    def __check_intersects(self, geom) -> Union[SingleGeom, None]:
        # Check if geom is geopandas dataframe object
        if isinstance(geom, gpd.GeoDataFrame):
            geo_json = geom.__geo_interface__
            return self.__get_subgeom(geo_json)
        # Check if geom is file path
        if isinstance(geom, Path):
            with open(geom.resolve(), "r") as fp:
                geo_json = json.load(fp)
            return self.__get_subgeom(geo_json)

    def __get_subgeom(self, geom_json) -> SingleGeom:
        return geom_json["features"][0]["geometry"]


def generate_client(
    catalog="LPCLOUD", url="https://cmr.earthdata.nasa.gov/stac/"
) -> Union[Client, None]:
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
    try:
        return CubeClient.open(f"{url}/{catalog}")
    except APIError:
        warnings.warn("STAC endpoint not found", RuntimeWarning)
        return None
