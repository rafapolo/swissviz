import os, sys

sys.path.insert(0, os.path.dirname(__file__))
from active_firms import parse_rows

CSV = (b"c,uid,name,type,street,number,zip,town,canton,seat\n"
       b"https://register.ld.admin.ch/zefix/company/694531,CHE109848706,Kaeon SA,"
       b"https://ld.admin.ch/ech/97/legalforms/0106,avenue du Belv\xc3\xa9d\xc3\xa8re,40,2025,Chez-le-Bart,NE,La Grande B\xc3\xa9roche\n"
       b"https://register.ld.admin.ch/zefix/company/7,CHE100000012,Ohne Adresse GmbH,"
       b"https://ld.admin.ch/ech/97/legalforms/0107,,,,,,\n")


def test_parse_rows_formats_uid_and_legal_form():
    df = parse_rows(CSV)
    r = df.row(0, named=True)
    assert r["uid"] == "CHE-109.848.706"
    assert r["legalForm"] == "0106"
    assert r["name"] == "Kaeon SA"
    assert (r["street"], r["houseNumber"], r["zip"], r["town"], r["canton"], r["seat"]) == \
           ("avenue du Belvédère", "40", "2025", "Chez-le-Bart", "NE", "La Grande Béroche")


def test_parse_rows_keeps_companies_without_address_as_empty_strings():
    r = parse_rows(CSV).row(1, named=True)
    assert r["uid"] == "CHE-100.000.012"
    assert r["legalForm"] == "0107"
    assert (r["street"], r["houseNumber"], r["zip"], r["town"], r["canton"], r["seat"]) == ("",) * 6


def test_parse_rows_one_row_per_uid():
    dup = CSV + CSV.split(b"\n", 1)[1]
    assert parse_rows(dup).height == 2
