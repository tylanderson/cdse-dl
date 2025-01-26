from datetime import datetime

import pytest
from shapely.geometry import MultiPolygon, Point

from cdse_dl.odata.search import (
    ProductSearch,
    _filter_from_datetime_components,
    _format_order_by,
    build_area_filter,
)
from cdse_dl.utils import parse_datetime_to_components
from cdse_dl.odata.utils import CopernicusODataError

@pytest.mark.default_cassette("search_s2_by_name.yaml")
@pytest.mark.vcr
def test_search():
    """Test search."""
    name = "S2A_MSIL1C_20200116T100341_N0500_R122_T33TUH_20230428T195719.SAFE"
    search = ProductSearch(name=name)
    search_params = search._parameters
    assert (
        search_params["filter"]
        == "Name eq 'S2A_MSIL1C_20200116T100341_N0500_R122_T33TUH_20230428T195719.SAFE'"
    )

    products = search.get(1)
    assert len(products) == 1
    assert products[0]["Name"] == name

    hits = search.hits()
    assert hits == 1

@pytest.mark.default_cassette("invalid_search_params.yaml")
@pytest.mark.vcr
def test_invalid_search_params():
    """Test invalid search params."""
    with pytest.raises(CopernicusODataError, match="Input should be greater than or equal to 0") as e:
        _ = ProductSearch(top=-1).get(1)
    assert "'loc': ['query', '$top']" in str(e.value)

    with pytest.raises(CopernicusODataError, match="Input should be less than or equal to 1000") as e:
        _ = ProductSearch(top=1001).get_all()
    assert "'loc': ['query', '$top']" in str(e.value)

    with pytest.raises(CopernicusODataError, match="Input should be greater than or equal to 0") as e:
        _ = ProductSearch(skip=-1).get(1)
    assert "'loc': ['query', '$skip']" in str(e.value)

    with pytest.raises(CopernicusODataError, match="Input should be less than or equal to 10000") as e:
        _ = ProductSearch(skip=10001).get(1)
    assert "'loc': ['query', '$skip']" in str(e.value)

    with pytest.raises(CopernicusODataError, match="Expand parameter only accepts following values:") as e:
        _ = ProductSearch(expand="test").get(1)

    with pytest.raises(CopernicusODataError, match="Invalid field name in the order by clause") as e:
        _ = ProductSearch(order_by="test").get(1)

    with pytest.raises(CopernicusODataError, match="Invalid value: test") as e:
        _ = ProductSearch(order="test").get(1)

    with pytest.raises(CopernicusODataError, match="Invalid field in select: test") as e:
        _ = ProductSearch(select=["test"]).get(1)


def test__format_order_by():
    """Test formatting order_by and order."""
    assert _format_order_by("order_by", "order") == "order_by order"
    assert _format_order_by("order_by", None) == "order_by"
    assert _format_order_by(None, "order") is None
    assert _format_order_by(None, None) is None


def test__parse_datetime_to_components():
    """Test conversion to components."""
    c = parse_datetime_to_components("2020-01-01/2020-01-02")
    assert c[0].isoformat() == "2020-01-01T00:00:00+00:00"
    assert c[1].isoformat() == "2020-01-02T00:00:00+00:00"

    c = parse_datetime_to_components("2020-01-01")
    assert c[0].isoformat() == "2020-01-01T00:00:00+00:00"
    assert c[1] is None

    c = parse_datetime_to_components([datetime(2020, 1, 1), datetime(2020, 1, 2)])
    assert c[0].isoformat() == "2020-01-01T00:00:00+00:00"
    assert c[1].isoformat() == "2020-01-02T00:00:00+00:00"

    c = parse_datetime_to_components(datetime(2020, 1, 1))
    assert c[0].isoformat() == "2020-01-01T00:00:00+00:00"
    assert c[1] is None

    c = parse_datetime_to_components(["2020-01-01", datetime(2020, 1, 2)])
    assert c[0].isoformat() == "2020-01-01T00:00:00+00:00"
    assert c[1].isoformat() == "2020-01-02T00:00:00+00:00"


def test__filter_from_datetime_components():
    """Test filter creation from datetimes."""
    # test two datetimes
    f = _filter_from_datetime_components(
        [datetime(2020, 1, 1), datetime(2020, 1, 2)], "date"
    )
    f.filter_string == "date ge 2020-01-01T00:00:00Z and date lt 2020-01-02T00:00:00Z"
    # test right open ended
    f = _filter_from_datetime_components([datetime(2020, 1, 1), None], "date")
    f.filter_string == "date ge 2020-01-01T00:00:00Z"
    # test left open ended
    f = _filter_from_datetime_components([None, datetime(2020, 1, 2)], "date")
    f.filter_string == "date lt 2020-01-02T00:00:00Z"
    # test double open ended
    with pytest.raises(Exception) as e:
        f = parse_datetime_to_components([None, None])
    assert str(e.value) == "cannot create a double open-ended interval"
    # test empty list
    with pytest.raises(Exception) as e:
        f = parse_datetime_to_components([])
    assert "too many/few datetime components" in str(e.value)
    # test more than 2 datetimes
    with pytest.raises(Exception) as e:
        f = parse_datetime_to_components(
            [datetime(2020, 1, 1), datetime(2020, 1, 2), datetime(2020, 1, 3)]
        )
    assert "too many/few datetime components" in str(e.value)


def test_build_area_filter():
    """Test area filter."""
    filter_truth = "OData.CSC.Intersects(area=geography'SRID=4326;POINT (0.0000000000000000 0.0000000000000000)')"
    # test WKT
    f = build_area_filter("POINT (0 0)")
    f.filter_string == filter_truth
    # test point
    f = build_area_filter({"type": "Point", "coordinates": [0, 0]})
    f.filter_string == filter_truth
    # test shapely
    f = build_area_filter(Point(0, 0))
    f.filter_string == filter_truth

    # test invalid wkt str
    with pytest.raises(ValueError) as e:
        f = build_area_filter("test")
    assert str(e.value) == "Could not parse str from wkt to geometry"
    # test invalid dict
    with pytest.raises(ValueError) as e:
        f = build_area_filter({"k": "v"})
    assert str(e.value) == "Could not parse dict to geometry"
    # test invalid multi-polygon
    with pytest.raises(ValueError) as e:
        f = build_area_filter(
            MultiPolygon(
                [
                    (
                        ((0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0)),
                        [((0.1, 0.1), (0.1, 0.2), (0.2, 0.2), (0.2, 0.1))],
                    )
                ]
            )
        )
    assert str(e.value) == "multipolygon not supported"
    # test invalid type
    with pytest.raises(ValueError) as e:
        f = build_area_filter(1.0)
    assert "Invalid value type" in str(e.value)
