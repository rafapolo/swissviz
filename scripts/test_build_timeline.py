import gzip, json, os, struct, sys

import polars as pl

sys.path.insert(0, os.path.dirname(__file__))
from build_timeline import NONE, build_lamps, day_of, entry_days, write_timeline


def events(rows):
    """SHAB events already placed on a building: (uid, kind, date, name, legalForm, lat, lng)."""
    return pl.DataFrame(rows, schema={"uid": pl.String, "kind": pl.String, "date": pl.String, "name": pl.String,
                                      "legalForm": pl.String, "lat": pl.Float64, "lng": pl.Float64}, orient="row")


def active(rows):
    """Active companies from LINDAS, geocoded: (uid, name, legalForm, lat, lng)."""
    return pl.DataFrame(rows, schema={"uid": pl.String, "name": pl.String, "legalForm": pl.String,
                                      "lat": pl.Float64, "lng": pl.Float64}, orient="row")


NO_ACTIVE = active([])


def lamp_rows(lamps):
    return lamps.select("name", "t", "o", "c").to_dicts()


def test_day_of_counts_from_1883():
    assert day_of(18830101) == 0
    assert day_of(18830102) == 1
    assert day_of(20180101) == 49308


def test_date_before_epoch_is_day_zero():
    assert day_of(18500101) == 0


def test_shab_lamp_keeps_its_days():
    ev = events([("CHE-111.111.111", "open", "2019-03-01", "A AG", "0106", 47.0, 8.0),
                 ("CHE-111.111.111", "close", "2021-06-30", "A AG", "0106", 47.0, 8.0)])
    lamps, _ = build_lamps(ev, {}, NO_ACTIVE)
    assert lamp_rows(lamps) == [{"name": "A AG", "t": "A", "o": day_of(20190301), "c": day_of(20210630)}]


def test_close_only_gets_entry_date():
    ev = events([("CHE-222.222.222", "close", "2020-05-04", "B GmbH", "0107", 46.5, 7.5),
                 ("CHE-333.333.333", "close", "2020-05-05", "C GmbH", "0107", 46.5, 7.5)])
    lamps, counts = build_lamps(ev, {222222222: day_of(19780630)}, NO_ACTIVE)
    rows = {r["name"]: r for r in lamp_rows(lamps)}
    assert rows["B GmbH"]["o"] == day_of(19780630)
    assert rows["C GmbH"]["o"] == NONE
    assert counts["close_only_dated"] == 1 and counts["close_only_undated"] == 1


def test_active_without_shab_event_gets_a_lamp():
    lamps, counts = build_lamps(events([]), {444444444: day_of(19050719)},
                                active([("CHE-444.444.444", "D SA", "0106", 46.2, 6.1)]))
    assert lamp_rows(lamps) == [{"name": "D SA", "t": "A", "o": day_of(19050719), "c": NONE}]
    r = lamps.row(0, named=True)
    assert (r["lat"], r["lng"]) == (46.2, 6.1)
    assert counts["active"] == 1


def test_uid_in_shab_and_active_is_one_lamp():
    ev = events([("CHE-555.555.555", "open", "2022-01-10", "E AG", "0106", 47.4, 8.5)])
    lamps, _ = build_lamps(ev, {555555555: day_of(20220110)},
                           active([("CHE-555.555.555", "E AG", "0106", 47.4, 8.5)]))
    assert lamps.height == 1
    assert lamp_rows(lamps)[0]["o"] == day_of(20220110)


def test_active_without_date_or_coordinate_is_dropped():
    lamps, counts = build_lamps(events([]), {777777777: day_of(19900101)},
                                active([("CHE-666.666.666", "F AG", "0106", 47.0, 8.0),    # no date
                                        ("CHE-777.777.777", "G AG", "0106", None, None)]))  # no coordinate
    assert lamps.height == 0
    assert counts["active_no_date"] == 1 and counts["active_no_geo"] == 1


def test_open_after_close_becomes_close_only():
    ev = events([("CHE-888.888.888", "close", "2019-02-01", "H AG", "0106", 47.0, 8.0)])
    lamps, counts = build_lamps(ev, {888888888: day_of(20200101)}, NO_ACTIVE)
    assert lamp_rows(lamps)[0]["o"] == NONE
    assert counts["open_after_close"] == 1


def test_unknown_legal_form_falls_back_to_b():
    lamps, _ = build_lamps(events([]), {999999999: day_of(19990101)},
                           active([("CHE-999.999.999", "I", None, 47.0, 8.0)]))
    assert lamp_rows(lamps)[0]["t"] == "B"


def test_estimates():
    assert entry_days({}, {1: (20070314, 1), 2: (2007, 2), 3: (0, 3)}) == \
           {1: day_of(20070314), 2: day_of(20070701)}
    assert entry_days({5: 19900101}, None) == {5: day_of(19900101)}
    # an exact register date beats an estimate
    assert entry_days({6: 19500101}, {6: (2001, 2)}) == {6: day_of(19500101)}


def test_binary_round_trip(tmp_path):
    lamps = pl.DataFrame({"name": ["X", "Y"], "t": ["A", "G"], "lat": [47.1, 46.9], "lng": [8.1, 7.9],
                          "o": [10, NONE], "c": [NONE, 50000]})
    write_timeline(lamps, str(tmp_path))
    buf = gzip.decompress((tmp_path / "events.bin.gz").read_bytes())
    n = struct.unpack_from("<I", buf)[0]
    assert n == 2
    o = struct.unpack_from("<2H", buf, 4 + 8 * n)
    c = struct.unpack_from("<2H", buf, 4 + 10 * n)
    assert o == (10, NONE) and c == (NONE, 50000)
    assert buf[4 + 12 * n:] == b"AG"
    assert json.loads(gzip.decompress((tmp_path / "names.json.gz").read_bytes())) == ["X", "Y"]
