import pytest
from cubed.utils import parse_bandmap


correct_band_map = {
    "HLSv2": {
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
            "B8A": "nir",
            "B10": "cirrus",
            "B11": "swir1",
            "B12": "swir2",
            "Fmask": "fmask",
        },
    }
}
band_map_path = "./cubed/bandmap.json"


def test_correct_band_map():
    collection = "HLSv2"
    band_map = parse_bandmap(collection, band_map_path)
    assert band_map == correct_band_map.get(collection)


def test_incorrect_band_map():
    band_map = parse_bandmap("wrong", band_map_path)
    assert band_map == None
