import gzip, json, os, struct, sys

import polars as pl

sys.path.insert(0, os.path.dirname(__file__))
from build_timeline import CANTON_IDS, NONE, build_lamps, day_of, entry_days, write_timeline


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


def test_alive_company_shines_at_its_current_address_and_name():
    ev = events([("CHE-555.555.555", "open", "2019-03-01", "Old Name GmbH", "0107", 47.0, 8.0)])
    lamps, counts = build_lamps(ev, {}, active([("CHE-555.555.555", "New Name AG", "0106", 46.5, 7.4)]))
    r = lamps.row(0, named=True)
    assert (r["name"], r["lat"], r["lng"], r["c"]) == ("New Name AG", 46.5, 7.4, NONE)
    assert r["canton"] == ""   # the test's LINDAS rows carry no canton
    assert counts["moved_to_current"] == 1


def test_closed_company_stays_where_it_closed():
    ev = events([("CHE-555.555.555", "open", "2019-03-01", "X GmbH", "0107", 47.0, 8.0),
                 ("CHE-555.555.555", "close", "2021-03-01", "X GmbH", "0107", 47.1, 8.1)])
    lamps, _ = build_lamps(ev, {}, active([("CHE-555.555.555", "X GmbH", "0107", 46.5, 7.4)]))
    r = lamps.row(0, named=True)
    assert (r["lat"], r["lng"]) == (47.0, 8.0)


def test_company_gone_from_lindas_goes_dark_on_the_snapshot_day():
    snap = day_of(20261014)
    ev = events([("CHE-666.666.666", "open", "2020-01-01", "Merged AG", "0106", 47.0, 8.0),
                 ("CHE-777.777.777", "open", "2026-10-01", "Brand New AG", "0106", 47.0, 8.0)])
    lamps, counts = build_lamps(ev, {}, active([("CHE-999.999.999", "Other AG", "0106", 46.0, 7.0)]), snap)
    c = {r["name"]: r["c"] for r in lamp_rows(lamps)}
    assert c["Merged AG"] == snap            # left the register with no SHAB deletion
    assert c["Brand New AG"] == NONE         # entered two weeks ago: LINDAS just hasn't caught up
    assert counts["left_without_deletion"] == 1


def test_lamp_canton_comes_from_lindas_when_alive_and_the_shab_when_closed():
    ev = events([("CHE-111.111.111", "open", "2019-03-01", "Moved AG", "0106", 47.0, 8.0),
                 ("CHE-222.222.222", "close", "2020-05-04", "Gone AG", "0106", 46.2, 6.1)]
                ).with_columns(cantons=pl.Series(["ZH", "GE"]))
    act = active([("CHE-111.111.111", "Moved AG", "0106", 46.9, 7.4)]).with_columns(canton=pl.lit("BE"))
    lamps, _ = build_lamps(ev, {}, act)
    assert dict(zip(lamps["name"], lamps["canton"])) == {"Moved AG": "BE", "Gone AG": "GE"}


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


def read_timeline(out_dir):
    """The decoding of loadTimeline in index.html: lamps as (name, t, lat, lng, o, c, canton)."""
    buf = gzip.decompress(open(f"{out_dir}/events.bin.gz", "rb").read())
    n, chunk, lat0, lng0, lat_step, lng_step = struct.unpack_from("<II4d", buf)
    p = 40
    def col(k):   # k-th uint16 column: low-byte plane, then high-byte plane
        lo, hi = buf[p + 2 * n * k: p + 2 * n * k + n], buf[p + 2 * n * k + n: p + 2 * n * (k + 1)]
        return [lo[i] | hi[i] << 8 for i in range(n)]
    def undelta(zz):
        out, prev = [], 0
        for z in zz:
            prev = (prev + ((z >> 1) ^ -(z & 1))) & 0xFFFF
            out.append(prev)
        return out
    qa, qn, o, c = undelta(col(0)), undelta(col(1)), col(2), col(3)
    t = buf[p + 8 * n: p + 9 * n].decode("ascii")
    canton = [CANTON_IDS[k] if k < len(CANTON_IDS) else "" for k in buf[p + 9 * n: p + 10 * n]]
    names = []
    for k in range((n + chunk - 1) // chunk):
        names += json.loads(gzip.decompress(open(f"{out_dir}/names/{k}.json.gz", "rb").read()))
    return [(names[i], t[i], lat0 + qa[i] * lat_step, lng0 + qn[i] * lng_step, o[i], c[i], canton[i])
            for i in range(n)]


def test_binary_round_trip(tmp_path):
    lamps = pl.DataFrame({"name": ["X", "Y"], "t": ["A", "G"], "canton": ["ZH", ""], "lat": [47.1, 46.9],
                          "lng": [8.1, 7.9], "o": [10, NONE], "c": [NONE, 50000]})
    write_timeline(lamps, str(tmp_path))
    got = sorted(read_timeline(tmp_path))
    assert [(r[0], r[1], r[4], r[5], r[6]) for r in got] == [("X", "A", 10, NONE, "ZH"), ("Y", "G", NONE, 50000, "")]
    assert abs(got[0][2] - 47.1) < 1e-9 and abs(got[1][3] - 7.9) < 1e-9   # box corners are exact


def test_binary_round_trip_across_chunks(tmp_path):
    """Lamps all over Switzerland, more than one names chunk: every lamp comes back
    with its name, form and days, within half a grid step of where it was."""
    import random
    import build_timeline
    rnd = random.Random(1)
    n = build_timeline.NAMES_CHUNK + 1000
    lamps = pl.DataFrame({"name": [f"F{i}" for i in range(n)], "t": [rnd.choice("ABGS") for _ in range(n)],
                          "canton": [rnd.choice(CANTON_IDS) for _ in range(n)],
                          "lat": [rnd.uniform(45.8, 47.8) for _ in range(n)],
                          "lng": [rnd.uniform(5.9, 10.5) for _ in range(n)],
                          "o": [rnd.randrange(NONE + 1) for _ in range(n)], "c": [rnd.randrange(NONE + 1) for _ in range(n)]})
    write_timeline(lamps, str(tmp_path))
    got = {r[0]: r for r in read_timeline(tmp_path)}
    assert len(got) == n
    for name, t, canton, lat, lng, o, c in lamps.iter_rows():
        g = got[name]
        assert (g[1], g[4], g[5], g[6]) == (t, o, c, canton)
        assert abs(g[2] - lat) < 2.0 / 65535 and abs(g[3] - lng) < 4.7 / 65535


def test_uid_without_dots_pairs_with_the_dotted_one():
    # the SHAB writes some UIDs bare (CHE492310367): opening and closing must still be one lamp
    ev = events([("CHE492310367", "open", "2019-01-15", "Ursuta AG", "0106", 46.1, 7.1),
                 ("CHE-492.310.367", "close", "2019-10-10", "Ursuta AG", "0106", 46.1, 7.1)])
    lamps, counts = build_lamps(ev, {}, NO_ACTIVE)
    assert lamp_rows(lamps) == [{"name": "Ursuta AG", "t": "A", "o": day_of(20190115), "c": day_of(20191010)}]
    assert counts["close_only_dated"] + counts["close_only_undated"] == 0
