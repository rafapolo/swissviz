import gzip, json, os, sys
from datetime import date

import polars as pl

sys.path.insert(0, os.path.dirname(__file__))
from fetch_cantonal import registered_dates, write_registered

COLS = ["uid", "host", "status", "registered", "first_journal", "has_report"]


def frame(rows):
    return pl.DataFrame(rows, schema={"uid": pl.String, "host": pl.String, "status": pl.String,
                                      "registered": pl.Date, "first_journal": pl.Date,
                                      "has_report": pl.Boolean}, orient="row")


def test_registered_dates_picks_one_date_per_uid():
    df = frame([
        ("CHE-105.909.036", "zg.chregister.ch", "ok", date(1883, 3, 9), date(1996, 1, 1), None),
        ("CHE-105.909.036", "prestations.vd.ch", "ok", date(1905, 7, 19), None, True),   # two registers: 1883 wins
        ("CHE-111.111.111", "adm.appls.fr.ch", "ok", None, date(1992, 11, 19), False),  # no date: journal
        ("CHE-222.222.222", "app2.ge.ch", "ok", None, date(2001, 5, 2), True),          # journal starts at migration: out
        ("CHE-333.333.333", "zh.chregister.ch", "not_found", None, None, None),         # out
    ])
    assert registered_dates(df).to_dicts() == [{"uid": 105909036, "date": 18830309},
                                               {"uid": 111111111, "date": 19921119}]


def test_registered_dates_before_1883_are_missing():
    # register typos such as year 0214 or 1190: treated as no date, not as the oldest company
    df = frame([
        ("CHE-444.444.444", "zh.chregister.ch", "ok", date(214, 2, 24), None, None),
        ("CHE-555.555.555", "be.chregister.ch", "ok", date(1190, 5, 1), date(1999, 3, 3), False),
    ])
    assert registered_dates(df).to_dicts() == [{"uid": 555555555, "date": 19990303}]


def test_write_registered_is_columnar_gzip(tmp_path):
    df = pl.DataFrame({"uid": [105909036, 111111111], "date": [18830309, 19921119]},
                      schema={"uid": pl.Int64, "date": pl.Int32})
    path = tmp_path / "registered.json.gz"
    write_registered(df, str(path))
    out = json.load(gzip.open(path))
    assert out["uid"] == [105909036, 111111111]
    assert out["date"] == [18830309, 19921119]
    assert len(out["asOf"]) == 10


def test_registered_dates_in_the_future_are_missing():
    # a register typo dated 2027 must not light a lamp after the timeline ends
    df = frame([
        ("CHE-666.666.666", "zh.chregister.ch", "ok", date(2027, 11, 1), date(2017, 11, 1), False),
    ])
    assert registered_dates(df).to_dicts() == [{"uid": 666666666, "date": 20171101}]
