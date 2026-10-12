"""Geocode Swiss addresses against the official swisstopo building address register.

The register (amtliches Gebäudeadressverzeichnis, ~3.4 M addresses) is downloaded
once into build/ and matched locally with Polars, so there are no API calls.

    from swisstopo_geo import geocode
    out = geocode(df)  # df needs street, houseNumber, zip, town, seat
    # adds lat, lng, prec (building | street | zip | seat | none)
"""
import os, re, unicodedata, urllib.request, zipfile

import polars as pl

URL = ("https://data.geo.admin.ch/ch.swisstopo.amtliches-gebaeudeadressverzeichnis/"
       "amtliches-gebaeudeadressverzeichnis_ch/amtliches-gebaeudeadressverzeichnis_ch_2056.csv.zip")
ZIP = "build/adr.zip"
CSV = "build/adr/amtliches-gebaeudeadressverzeichnis_ch_2056.csv"
PARQUET = "build/adr.parquet"

ABBREV = [
    (r"str\.?$", "strasse"), (r"str\.?\s", "strasse "), (r"\bstr\b", "strasse"),
    (r"\bav\.?\s", "avenue "), (r"\bch\.?\s", "chemin "), (r"\brte\.?\s", "route "),
    (r"\bbd\.?\s", "boulevard "), (r"\bpl\.?\s", "place "), (r"\bst\.?\s", "sankt "),
]


# words a street name can carry or drop: "Rue de l'Hôtel-de-Ville" is
# "Rue Hôtel-de-Ville" in the register, "Im Schaiengarten" is "Schaiengarten"
PARTICLES = {"im", "in", "am", "an", "auf", "zum", "zur", "der", "die", "das", "de", "du", "des",
             "la", "le", "les", "l", "d", "al", "alla", "del", "della", "dei", "di", "da"}


def _words(s: str) -> list:
    s = s.casefold().replace("ß", "ss")
    s = s.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.replace("'", " ").replace("’", " ").replace("-", " ")
    for pat, rep in ABBREV:
        s = re.sub(pat, rep, s)
    return re.findall(r"[a-z0-9]+", s)


def core_street(s: str) -> str:
    return "".join(w for w in _words(s or "") if w not in PARTICLES)


def norm_street(s: str) -> str:
    if not s:
        return ""
    s = s.casefold().replace("ß", "ss")
    s = s.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.replace("'", " ").replace("’", " ").replace("-", " ")
    for pat, rep in ABBREV:
        s = re.sub(pat, rep, s)
    return re.sub(r"[^a-z0-9]", "", s)


def norm_num(s: str) -> str:
    """"12c" → "12c", "42-48" / "32/36" → "42", "140 D" → "140d"."""
    s = re.sub(r"\s+", "", (s or "").casefold())
    m = re.match(r"\d+(\.\d+)?[a-z]?(?![0-9])", s)
    return m.group(0) if m else s


def norm_place(s: str) -> str:
    s = re.sub(r"\s*\(.*?\)|\s+[A-Z]{2}$", "", s or "")  # "Wil (SG)", "Wil SG"
    return norm_street(s)


def lv95_to_wgs84(e, n):
    """swisstopo approximate formulas, ~1 m accuracy."""
    y = (e - 2_600_000) / 1e6
    x = (n - 1_200_000) / 1e6
    lng = (2.6779094 + 4.728982 * y + 0.791484 * y * x + 0.1306 * y * x * x
           - 0.0436 * y ** 3) * 100 / 36
    lat = (16.9023892 + 3.238272 * x - 0.270978 * y * y - 0.002528 * x * x
           - 0.0447 * y * y * x - 0.0140 * x ** 3) * 100 / 36
    return lat, lng


def load_register() -> pl.DataFrame:
    if os.path.exists(PARQUET):
        return pl.read_parquet(PARQUET)
    if not os.path.exists(CSV):
        os.makedirs("build/adr", exist_ok=True)
        if not os.path.exists(ZIP):
            urllib.request.urlretrieve(URL, ZIP)
        zipfile.ZipFile(ZIP).extractall("build/adr")
    df = pl.read_csv(CSV, separator=";", infer_schema=False, encoding="utf8-lossy")
    df = df.rename({df.columns[0]: df.columns[0].lstrip("﻿")})
    # "Mühlebrücke / Pont-du-Moulin": one row per language
    df = df.with_columns(pl.col("STN_LABEL").fill_null("").str.split(" / ")).explode("STN_LABEL")
    e = df["ADR_EASTING"].cast(pl.Float64)
    n = df["ADR_NORTHING"].cast(pl.Float64)
    lat, lng = lv95_to_wgs84(e, n)
    df = pl.DataFrame({
        "s": df["STN_LABEL"].map_elements(norm_street, return_dtype=pl.Utf8),
        "sc": df["STN_LABEL"].map_elements(core_street, return_dtype=pl.Utf8),
        "h": df["ADR_NUMBER"].fill_null("").map_elements(norm_num, return_dtype=pl.Utf8),
        "z": df["ZIP_LABEL"].str.slice(0, 4),
        "loc": df["ZIP_LABEL"].str.slice(5).map_elements(norm_place, return_dtype=pl.Utf8),
        "com": df["COM_NAME"].map_elements(norm_place, return_dtype=pl.Utf8),
        "real": df["ADR_STATUS"] == "real",
        "lat": lat, "lng": lng,
    }).filter(pl.col("lat").is_not_null())
    df.write_parquet(PARQUET)
    return df


def geocode(ev: pl.DataFrame) -> pl.DataFrame:
    reg = load_register().sort("real", descending=True)
    q = ev.with_row_index("_i").with_columns(
        pl.col("street").map_elements(norm_street, return_dtype=pl.Utf8).alias("s"),
        pl.col("street").map_elements(core_street, return_dtype=pl.Utf8).alias("sc"),
        pl.col("houseNumber").map_elements(norm_num, return_dtype=pl.Utf8).alias("h"),
        pl.col("zip").alias("z"),
        pl.col("town").map_elements(norm_place, return_dtype=pl.Utf8).alias("loc"),
        pl.col("seat").map_elements(norm_place, return_dtype=pl.Utf8).alias("com"),
    )

    def first(keys):
        return reg.group_by(keys, maintain_order=True).agg(pl.col("lat").first(), pl.col("lng").first())

    def centroid(keys):
        return reg.group_by(keys).agg(pl.col("lat").median(), pl.col("lng").median())

    steps = [
        ("building", ["z", "s", "h"], first),
        ("building", ["loc", "s", "h"], first),
        ("building", ["com", "s", "h"], first),
        ("building", ["z", "sc", "h"], first),
        ("building", ["com", "sc", "h"], first),
        ("street", ["z", "s"], centroid),
        ("street", ["loc", "s"], centroid),
        ("street", ["z", "sc"], centroid),
        ("zip", ["z"], centroid),
        ("seat", ["com"], centroid),
        ("seat", ["loc"], centroid),
    ]
    found = []
    left = q
    for prec, keys, agg in steps:
        cand = left.filter(pl.all_horizontal([pl.col(k) != "" for k in keys]))
        hit = cand.join(agg(keys).filter(pl.all_horizontal([pl.col(k) != "" for k in keys])),
                        on=keys, how="inner")
        found.append(hit.select("_i", "lat", "lng", pl.lit(prec).alias("prec")))
        left = left.join(hit.select("_i"), on="_i", how="anti")
    hits = pl.concat(found)
    return (ev.with_row_index("_i").join(hits, on="_i", how="left")
            .with_columns(pl.col("prec").fill_null("none")).drop("_i"))
