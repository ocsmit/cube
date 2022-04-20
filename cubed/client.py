from pystac_client import Client
from pystac_client.item_search import ItemSearch
import geopandas as gpd
from typing import Union, Any, Dict, Tuple
from pathlib import Path
import json


class CubeClient(Client):
    def __init__(self, **kwargs: Any) -> None:
        super(Client, self).__init__(**kwargs)

    def search(self, **kwargs: Any) -> ItemSearch:
        if "intersects" in list(kwargs.keys()):
            geom = kwargs.get("intersects")
            kwargs["intersects"] = self.__check_intersects(geom)
        return super(CubeClient, self).search(**kwargs)

    def __check_intersects(self, geom: Union[) -> Dict[str, Union[str, Tuple]]:
        # Check if geom is geopandas dataframe object
        if isinstance(geom, gpd.GeoDataFrame):
            geo_json = geom.__geo_interface__
            return self.__get_subgeom(geo_json)
        # Check if geom is file path
        if isinstance(geom, Path):
            with open(geom.resolve(), "r") as fp:
                geo_json = json.load(fp)
            return self.__get_subgeom(geo_json)

    def __get_subgeom(self, geom_json) -> Dict[str, Union[str, Tuple]]:
        return geom_json["features"][0]["geometry"]
