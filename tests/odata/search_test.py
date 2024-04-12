from datetime import datetime

import pytest
from shapely.geometry import MultiPolygon, Point

from cdse_dl.odata.search import (
    ProductSearch,
    _filter_from_datetime_components,
    _format_order_by,
    _parse_datetime_to_components,
    build_area_filter,
)


@pytest.mark.default_cassette("search_s2_by_name.yaml")
@pytest.mark.vcr
def test_search():
    """Test search."""
    name = "S2B_MSIL1C_20210711T095029_N0301_R079_T34UEC_20210711T110140.SAFE"
    search = ProductSearch(name=name)
    search_params = search._parameters
    assert (
        search_params["filter"]
        == "Name eq 'S2B_MSIL1C_20210711T095029_N0301_R079_T34UEC_20210711T110140.SAFE'"
    )

    products = search.get(1)
    assert len(products) == 1
    assert products[0]["Name"] == name

    hits = search.hits()
    assert hits == 1


def test_invalid_search_params():
    """Test invalid search params."""
    with pytest.raises(ValueError) as e:
        _ = ProductSearch(top=-1)
    assert str(e.value) == "top must be between 0 and 1000"

    with pytest.raises(ValueError) as e:
        _ = ProductSearch(top=1001)
    assert str(e.value) == "top must be between 0 and 1000"

    with pytest.raises(ValueError) as e:
        _ = ProductSearch(skip=-1)
    assert str(e.value) == "skip must be between 0 and 10000"

    with pytest.raises(ValueError) as e:
        _ = ProductSearch(skip=10001)
    assert str(e.value) == "skip must be between 0 and 10000"

    with pytest.raises(ValueError) as e:
        _ = ProductSearch(expand="test")
    assert "Invalid `expand` " in str(e.value)

    with pytest.raises(ValueError) as e:
        _ = ProductSearch(order_by="test")
    assert "Invalid `order_by` " in str(e.value)

    with pytest.raises(ValueError) as e:
        _ = ProductSearch(order="test")
    assert "Invalid `order` " in str(e.value)


def test__format_order_by():
    """Test formatting order_by and order."""
    assert _format_order_by("order_by", "order") == "order_by order"
    assert _format_order_by("order_by", None) == "order_by"
    assert _format_order_by(None, "order") is None
    assert _format_order_by(None, None) is None


def test__parse_datetime_to_components():
    """Test conversion to components."""
    c = _parse_datetime_to_components("2020-01-01/2020-01-02")
    assert len(c) == 2
    assert c[0].isoformat() == "2020-01-01T00:00:00+00:00"
    assert c[1].isoformat() == "2020-01-02T00:00:00+00:00"

    c = _parse_datetime_to_components("2020-01-01")
    assert len(c) == 1
    assert c[0].isoformat() == "2020-01-01T00:00:00+00:00"

    c = _parse_datetime_to_components([datetime(2020, 1, 1), datetime(2020, 1, 2)])
    assert len(c) == 2
    assert c[0].isoformat() == "2020-01-01T00:00:00+00:00"
    assert c[1].isoformat() == "2020-01-02T00:00:00+00:00"

    c = _parse_datetime_to_components(datetime(2020, 1, 1))
    assert len(c) == 1
    assert c[0].isoformat() == "2020-01-01T00:00:00+00:00"

    c = _parse_datetime_to_components(["2020-01-01", datetime(2020, 1, 2)])
    assert len(c) == 2
    assert c[0].isoformat() == "2020-01-01T00:00:00+00:00"
    assert c[1].isoformat() == "2020-01-02T00:00:00+00:00"


def test__filter_from_datetime_components():
    """Test filter creation from datetimes."""
    # test two datetimes
    f = _filter_from_datetime_components(
        [datetime(2020, 1, 1), datetime(2020, 1, 2)], "date"
    )
    f.filter_string == "date ge 2020-01-01T00:00:00Z and date lt 2020-01-02T00:00:00Z"
    # test one datetime
    f = _filter_from_datetime_components([datetime(2020, 1, 1)], "date")
    f.filter_string == "date ge 2020-01-01T00:00:00Z"
    # test right open ended
    f = _filter_from_datetime_components([datetime(2020, 1, 1), None], "date")
    f.filter_string == "date ge 2020-01-01T00:00:00Z"
    # test left open ended
    f = _filter_from_datetime_components([None, datetime(2020, 1, 2)], "date")
    f.filter_string == "date lt 2020-01-02T00:00:00Z"
    # test double open ended
    with pytest.raises(Exception) as e:
        f = _filter_from_datetime_components([None, None], "date")
    assert str(e.value) == "cannot create a double open-ended interval"
    # test empty list
    with pytest.raises(Exception) as e:
        f = _filter_from_datetime_components([], "date")
    assert "too many/few datetime components" in str(e.value)
    # test more than 2 datetimes
    with pytest.raises(Exception) as e:
        f = _filter_from_datetime_components(
            [datetime(2020, 1, 1), datetime(2020, 1, 2), datetime(2020, 1, 3)], "date"
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
