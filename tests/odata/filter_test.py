from datetime import datetime

from dateutil import tz

from cdse_dl.odata.filter import Filter, make_datetime_utc


def test_make_datetime_utc():
    naive_datetime = datetime(2020, 1, 1)
    dt = make_datetime_utc(naive_datetime)
    assert dt.isoformat() == "2020-01-01T00:00:00+00:00"

    est = tz.gettz("America/New_York")
    aware_datetime = datetime(2020, 1, 1, 12, 1, 1, tzinfo=est)
    dt = make_datetime_utc(aware_datetime)
    assert dt.isoformat() == "2020-01-01T17:01:01+00:00"


def test_filter_format():
    # test int and float
    assert Filter.format_value(1) == "1"
    assert Filter.format_value(1.0) == "1.0"

    # test datetime
    dt = datetime(2020, 1, 1)
    assert Filter.format_value(dt) == "2020-01-01T00:00:00Z"

    # test string
    assert Filter.format_value("test") == "'test'"
