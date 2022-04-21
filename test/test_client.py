import pytest
from cubed.client import generate_client

correct_stac = {"catalog": "LPCLOUD", "url": "https://cmr.earthdata.nasa.gov/stac/"}
wrong_stac = {"catalog": "wrong", "url": "https://cmr.earthdata.nasa.gov/stac/"}


def test_client_correct():
    client = generate_client(**correct_stac)
    assert client.id == correct_stac.get("catalog")


@pytest.mark.filterwarnings("ignore:STAC endpoint")
def test_client_wrong():
    client = generate_client(**wrong_stac)
    assert client == None
