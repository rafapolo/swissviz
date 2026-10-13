#!/usr/bin/env python3
"""Entry date and history of companies from the cantonal commercial registers.

Zefix gives no entry date; the cantonal excerpt does ("Eingetragen am", back to
1883), plus the deletion date and the history of name, seat, address and
capital. See tasks/cantonal-register.md.

Two systems cover every canton:
  - chregister (DV Bern, JSF/PrimeFaces): {c}.chregister.ch, 24 registers,
    one server. GET the excerpt page, then POST the deferred-panel AJAX load.
  - hrc-internet (Angular + JSON): GE, VD, FR, NE. GET the catalog config for
    the XSRF cookie, then POST /api/public/hrcexcerpts/.

Collection is slow and identifiable: one request at a time per lane, >=1 s
between requests, exponential backoff on 403/429/5xx, User-Agent
"cantography-research" and no contact. chregister runs CHREG_LANES lanes on
its shared server (4; 8 was tried on 2026-10-13 and was no faster), each romand host one lane.
On 2026-10-13 P1 was split over three machines (--split 3, 4 lanes each).
A host stops itself above 5% errors.

Only extracted fields are cached, one JSON per company and register:
  build/cantonal/{host}/{uid}.json
No people: only the count of entries in the people table is kept.

Usage:
  python3 scripts/fetch_cantonal.py --jobs p1           # closed 2018-2026, no known opening
  python3 scripts/fetch_cantonal.py --jobs sample       # C0 stratified sample (~50)
  python3 scripts/fetch_cantonal.py --split 3           # remaining p1 jobs -> build/p1_part{0,1,2}.json
  python3 scripts/fetch_cantonal.py --jobs-file build/p1_part0.json   # one part per machine
  python3 scripts/fetch_cantonal.py --parquet           # cache -> build/cantonal.parquet
"""
import argparse, collections, datetime, glob, html, json, os, queue, random, re, sys, threading, time
import polars as pl
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BUILD = os.path.join(ROOT, "build")
CACHE = os.path.join(BUILD, "cantonal")
UA = "cantography-research"
PAUSE = 1.0
CHREG_LANES = 4
MAX_ERR = 0.05

# registry office (Zefix registerOfficeId, CHID digits 3-5) -> host, read from Zefix on 2026-10-13
OFFICE_HOST = {
    20: "zh.chregister.ch", 36: "be.chregister.ch", 100: "lu.chregister.ch", 120: "ur.chregister.ch",
    130: "sz.chregister.ch", 140: "ow.chregister.ch", 150: "nw.chregister.ch", 160: "gl.chregister.ch",
    170: "zg.chregister.ch", 217: "adm.appls.fr.ch", 241: "so.chregister.ch", 270: "bs.chregister.ch",
    280: "bl.chregister.ch", 290: "sh.chregister.ch", 300: "ar.chregister.ch", 310: "ai.chregister.ch",
    320: "sg.chregister.ch", 350: "gr.chregister.ch", 400: "ag.chregister.ch", 440: "tg.chregister.ch",
    501: "ti.chregister.ch", 550: "prestations.vd.ch", 600: "vo.chregister.ch", 621: "vb.chregister.ch",
    626: "vc.chregister.ch", 645: "rcnet.ne.ch", 660: "app2.ge.ch", 670: "ju.chregister.ch",
}
HRC_BASE = {
    "app2.ge.ch": "https://app2.ge.ch/ecohrcinternet",
    "prestations.vd.ch": "https://prestations.vd.ch/pub/101266",
    "adm.appls.fr.ch": "https://adm.appls.fr.ch/hrcmatic",
    "rcnet.ne.ch": "https://rcnet.ne.ch",
}
CANTON_HOST = {"GE": "app2.ge.ch", "VD": "prestations.vd.ch", "FR": "adm.appls.fr.ch", "NE": "rcnet.ne.ch"}
for _c in "ZH BE LU UR SZ OW NW GL ZG SO BS BL SH AR AI SG GR AG TG TI JU".split():
    CANTON_HOST[_c] = f"{_c.lower()}.chregister.ch"


def vs_hosts(lng):
    """Valais has three registers by district; without the municipality number,
    guess by longitude (Bas-Valais west of Riddes, Oberwallis east of the
    Raspille) and fall back to the others when the excerpt is not found."""
    order = ["vb.chregister.ch", "vc.chregister.ch", "vo.chregister.ch"]
    if lng is None:
        return ["vc.chregister.ch", "vb.chregister.ch", "vo.chregister.ch"]
    centre = {"vb.chregister.ch": 7.0, "vc.chregister.ch": 7.4, "vo.chregister.ch": 7.9}
    return sorted(order, key=lambda h: abs(centre[h] - lng))


# ── parsing ───────────────────────────────────────────────────────────────────
def text(s):
    s = re.sub(r"<br\s*/?>", "\n", s)
    s = re.sub(r'<span class="hidden">.*?</span>', "", s, flags=re.S)
    s = html.unescape(re.sub(r"<[^>]+>", " ", s)).replace("\xa0", " ")
    return "\n".join(re.sub(r"[ \t]+", " ", l).strip() for l in s.split("\n") if l.strip()).strip()


def ddmmyyyy(s):
    m = re.search(r"(\d{2})\.(\d{2})\.(\d{4})", s or "")
    return f"{m.group(3)}-{m.group(2)}-{m.group(1)}" if m else None


def ref(s):
    m = re.search(r"\d+", s or "")
    return int(m.group()) if m else None


def parse_chregister(x):
    """Partial-response XML of the excerpt panel -> fields, or None if no excerpt."""
    i = x.find('id="Titel"')
    if i < 0:
        return None
    head_end = x.find('<table', i)
    head = x[i:head_end]
    ps = [text(p) for p in re.findall(r"<p>(.*?)</p>", head, flags=re.S)]
    ps = [p for p in ps if p]
    out = {"name": ps[0] if ps else None, "legal_form": ps[1] if len(ps) > 1 else None,
           "seat": ps[2] if len(ps) > 2 else None}
    t = text(head)
    m = re.search(r"Eingetragen am\s+(\d{2}\.\d{2}\.\d{4})", t)
    out["registered"] = ddmmyyyy(m.group(1)) if m else None
    m = re.search(r"Gelöscht am\s+(\d{2}\.\d{2}\.\d{4})", t)
    out["deleted"] = ddmmyyyy(m.group(1)) if m else None
    m = re.search(r"Übertrag\w* (?:von|vom)\s+(\d{2}\.\d{2}\.\d{4})", text(x))
    out["migrated"] = ddmmyyyy(m.group(1)) if m else None
    m = re.search(r'data-uid="true"[^>]*>([^<]+)<', head)
    out["uid_shown"] = m.group(1).strip() if m else None

    names, seats, addresses, capital, journal, forms = [], [], [], [], [], []
    purpose, n_persons = None, None
    for tb in re.findall(r"<table.*?</table>", x[head_end:], flags=re.S):
        ths = [text(h) for h in re.findall(r"<th[^>]*>(.*?)</th>", tb, flags=re.S)]
        rows = []
        for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", tb, flags=re.S):
            tds = re.findall(r"<td[^>]*>(.*?)</td>", tr, flags=re.S)
            if tds:
                rows.append(([text(c) for c in tds], 'class="strike' in tr))
        if not ths:
            continue
        key = ths[-1] if ths[-1] else (ths[-2] if len(ths) > 1 else "")
        cols = ths
        if "TR-Nr" in cols:
            for c, _ in rows:
                if len(c) >= 5 and ref(c[1]):
                    journal.append({"ref": ref(c[0]), "tr_nr": ref(c[1]), "tr_date": ddmmyyyy(c[2]),
                                    "shab_nr": ref(c[3]), "shab_date": ddmmyyyy(c[4])})
            continue
        if any("Personalangaben" in h for h in cols):
            n_persons = sum(1 for c, _ in rows if any(v for v in c))
            continue

        def history(target):
            for c, struck in rows:
                if len(c) < 2 or not c[-1]:
                    continue
                if cols[0] == "Ref":
                    target.append({"value": c[-1], "ei": ref(c[0]), "lo": None})
                else:
                    target.append({"value": c[-1], "ei": ref(c[0]), "lo": ref(c[1]) if len(c) > 2 else None})
        # branches (Zweigniederlassung) name the same columns "... der Zweigniederlassung"
        if key.startswith("Firma") and cols[0] in ("Ei", "Ref"):
            history(names)
        elif key.startswith("Sitz"):
            history(seats)
        elif key in ("Domiziladresse", "Adresse der Zweigniederlassung"):
            history(addresses)
        elif key.startswith("Rechtsform"):
            history(forms)
        elif key.startswith("Zweck"):
            vals = [c[-1] for c, struck in rows if c and c[-1] and not struck]
            purpose = vals[-1] if vals else None
        elif "kapital" in "".join(cols).lower() and "Gesellschafter" not in "".join(cols) \
                and "Kommanditär" not in "".join(cols):
            ci = next(k for k, h in enumerate(cols) if "kapital" in h.lower())
            for c, struck in rows:
                if len(c) > ci and c[ci]:
                    capital.append({"value": c[ci], "ei": ref(c[0]),
                                    "lo": ref(c[1]) if cols[1] in ("Lö",) else None})
    out.update(names=names, seats=seats, addresses=addresses, legal_forms=forms, capital=capital,
               journal=journal, purpose=purpose, n_persons=n_persons)
    return out


# the dates come in the firm's language: FR has German-speaking firms
MONTHS = {"janvier": 1, "février": 2, "mars": 3, "avril": 4, "mai": 5, "juin": 6, "juillet": 7,
          "août": 8, "septembre": 9, "octobre": 10, "novembre": 11, "décembre": 12,
          "januar": 1, "februar": 2, "märz": 3, "april": 4, "juni": 6, "juli": 7, "august": 8,
          "oktober": 10, "dezember": 12}


def fr_date(s):
    m = re.match(r"\s*(\d{1,2})\.?\s+(\S+)\s+(\d{4})", s or "")
    if not m or m.group(2).lower() not in MONTHS:
        return ddmmyyyy(s)
    return f"{m.group(3)}-{MONTHS[m.group(2).lower()]:02d}-{int(m.group(1)):02d}"


def parse_hrc(j):
    ents = j.get("ents") or []
    if not ents:
        return None
    e = ents[0]

    def hist(rows):
        return [{"value": r.get("libelle"), "ei": r.get("noReference"),
                 "lo": None, "struck": r.get("codeEtat") == "R"} for r in rows or []]
    names = hist(j.get("rsos"))
    jr = []
    migrated = False
    for r in j.get("jrns") or []:
        if r.get("date") == "report":
            migrated = True
            continue
        jr.append({"ref": r.get("noReference"), "tr_nr": ref(r.get("no")), "tr_date": ddmmyyyy(r.get("date")),
                   "shab_nr": ref(r.get("pubPageId")), "shab_date": ddmmyyyy(r.get("pubDate"))})
    purpose = next((o.get("libelle") for o in j.get("objs") or []
                    if o.get("rub") == "But:" and o.get("codeEtat") == "A"), None)
    n_persons = len({a.get("hashCode") for a in j.get("adms") or []})
    return {"name": e.get("raisonSociale"), "legal_form": e.get("natureJuridique"), "seat": None,
            "registered": fr_date(e.get("dateInscription")), "deleted": fr_date(e.get("dateRadiation")),
            "migrated": None, "has_report": migrated, "uid_shown": e.get("uidOfs"), "chid": e.get("noDossierFed"),
            "names": names, "seats": hist(j.get("sies")), "addresses": hist(j.get("adrs")),
            "legal_forms": [], "capital": [{"value": c.get("capitalNominal") or c.get("libelle"),
                                            "ei": c.get("noReference"), "lo": None,
                                            "struck": c.get("codeEtat") == "R"} for c in j.get("caps") or []],
            "journal": jr, "purpose": purpose, "n_persons": n_persons}


# ── fetching ──────────────────────────────────────────────────────────────────
class Blocked(Exception):
    pass


class Lane:
    """One request at a time, >= PAUSE apart, with backoff."""

    def __init__(self):
        self.s = requests.Session()
        self.s.headers.update({"User-Agent": UA, "Accept-Language": "de-CH,de;q=0.9"})
        self.last = 0.0
        self.xsrf = {}

    def req(self, method, url, **kw):
        for attempt in range(5):
            wait = self.last + PAUSE - time.time()
            if wait > 0:
                time.sleep(wait)
            try:
                r = self.s.request(method, url, timeout=60, **kw)
            except requests.RequestException as e:
                r, err = None, str(e)
            self.last = time.time()
            if r is not None and r.status_code < 400:
                return r
            code = r.status_code if r is not None else None
            if code in (403, 429) and attempt >= 2:
                raise Blocked(f"{code} {url}")
            if code is not None and 400 <= code < 500 and code not in (403, 408, 429):
                raise RuntimeError(f"{code} {url}")
            time.sleep(min(300, 5 * 2 ** attempt) + random.random())
            if code == 403 and "hrcexcerpts" in url:
                return r   # let the caller renew the XSRF token
        raise RuntimeError(f"failed {method} {url}: {code if r is not None else err}")

    def chregister(self, host, uid, deleted_tr=None):
        url = f"https://{host}/cr-portal/auszug/auszug.xhtml?uid={uid}"
        if deleted_tr:
            url += f"&loeschung={deleted_tr.replace('-', '')}"
        h = self.req("GET", url).text
        vs = re.search(r'id="j_id1:javax.faces.ViewState:2" value="([^"]+)"', h)
        nonce = re.search(r'nonce="([^"]+)"', h)
        if not vs or 'auszugContentPanel' not in h:
            raise RuntimeError(f"no excerpt form at {url}")
        p = "idAuszugForm:auszugContentPanel"
        data = {"javax.faces.partial.ajax": "true", "javax.faces.source": p, "primefaces.ignoreautoupdate": "true",
                "javax.faces.partial.execute": p, "javax.faces.partial.render": p, p: p, p + "_load": "true",
                "idAuszugForm": "idAuszugForm", "javax.faces.ViewState": vs.group(1)}
        if nonce:
            data["primefaces.nonce"] = nonce.group(1)
        x = self.req("POST", f"https://{host}/cr-portal/auszug/auszug.xhtml", data=data,
                     headers={"Faces-Request": "partial/ajax", "X-Requested-With": "XMLHttpRequest",
                              "Accept": "application/xml, text/xml, */*; q=0.01"}).text
        if not x.startswith("<?xml") or "partial-response" not in x[:200]:
            raise RuntimeError("not a partial response")
        out = parse_chregister(x)
        if out is None and len(x) > 20000:
            raise RuntimeError("excerpt markup not recognised")
        return out

    def hrc(self, host, uid):
        base = HRC_BASE[host]
        for attempt in range(2):
            if host not in self.xsrf:
                self.req("GET", base + "/api/public/catalog/configuration")
                self.xsrf[host] = self.s.cookies.get("XSRF-TOKEN", domain=host)
            r = self.req("POST", base + "/api/public/hrcexcerpts/",
                         json={"rcentId": "", "lng": "FR", "rad": True, "companyOfsUid": uid,
                               "extraitTravail": False, "admOrderDirection": "ASC", "order": "F"},
                         headers={"X-XSRF-TOKEN": self.xsrf[host] or "", "Referer": base + "/",
                                  "Origin": "https://" + host, "Accept": "application/json, text/plain, */*"})
            if r.status_code == 403:
                self.xsrf.pop(host, None)
                continue
            return parse_hrc(r.json())
        raise Blocked(f"403 after XSRF renewal {host}")


def cache_path(host, uid):
    return os.path.join(CACHE, host, uid + ".json")


def done(job):
    return any(os.path.exists(cache_path(h, job["uid"])) for h in job["hosts"])


class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.c = collections.defaultdict(collections.Counter)
        self.stopped = set()
        self.t0 = time.time()

    def add(self, host, kind, dt=0.0):
        with self.lock:
            self.c[host][kind] += 1
            self.c[host]["sec"] += dt
            n = sum(v for k, v in self.c[host].items() if k != "sec")
            if n >= 40 and self.c[host]["error"] / n > MAX_ERR and host not in self.stopped:
                self.stopped.add(host)
                print(f"!! {host}: {self.c[host]['error']}/{n} errors, stopping this host", flush=True)

    def report(self):
        with self.lock:
            for h in sorted(self.c):
                c = self.c[h]
                n = c["ok"] + c["not_found"] + c["error"]
                print(f"  {h:22} ok {c['ok']:6} nf {c['not_found']:5} err {c['error']:4} "
                      f"{c['sec'] / max(1, n):5.1f} s/firm{' STOPPED' if h in self.stopped else ''}", flush=True)


def write(host, uid, rec):
    os.makedirs(os.path.join(CACHE, host), exist_ok=True)
    tmp = cache_path(host, uid) + ".tmp"
    with open(tmp, "w") as f:
        json.dump(rec, f, ensure_ascii=False)
    os.replace(tmp, cache_path(host, uid))


def work(lane, q, stats, log):
    while True:
        try:
            job = q.get_nowait()
        except queue.Empty:
            return
        uid = job["uid"]
        for k, host in enumerate(job["hosts"]):
            if host in stats.stopped:
                break
            t = time.time()
            try:
                if host in HRC_BASE:
                    out = lane.hrc(host, uid)
                else:
                    out = lane.chregister(host, uid, job.get("deleted_tr"))
            except Blocked as e:
                print(f"!! blocked: {e}", flush=True)
                stats.add(host, "error", time.time() - t)
                with stats.lock:
                    stats.stopped.add(host)
                break
            except Exception as e:
                stats.add(host, "error", time.time() - t)
                log.write(json.dumps({"uid": uid, "host": host, "error": str(e)[:300]}) + "\n")
                log.flush()
                break
            dt = time.time() - t
            if out is None and k + 1 < len(job["hosts"]):
                stats.add(host, "not_found", dt)
                continue   # Valais: try the next register
            rec = {"uid": uid, "host": host, "status": "ok" if out else "not_found",
                   "fetched": datetime.date.today().isoformat(), **(out or {})}
            write(host, uid, rec)
            stats.add(host, rec["status"], dt)
            break


def run(jobs):
    jobs = [j for j in jobs if not done(j)]
    print(f"{len(jobs)} firms to fetch", flush=True)
    chreg, hrc = queue.Queue(), collections.defaultdict(queue.Queue)
    for j in jobs:
        (hrc[j["hosts"][0]] if j["hosts"][0] in HRC_BASE else chreg).put(j)
    stats = Stats()
    log = open(os.path.join(BUILD, "cantonal_errors.jsonl"), "a")
    threads = [threading.Thread(target=work, args=(Lane(), chreg, stats, log), daemon=True)
               for _ in range(CHREG_LANES)]
    threads += [threading.Thread(target=work, args=(Lane(), q, stats, log), daemon=True) for q in hrc.values()]
    for t in threads:
        t.start()
    last = time.time()
    while any(t.is_alive() for t in threads):
        time.sleep(5)
        if time.time() - last > 300:
            last = time.time()
            print(f"-- {datetime.datetime.now():%H:%M} left: chregister {chreg.qsize()} "
                  + " ".join(f"{h.split('.')[0]} {q.qsize()}" for h, q in hrc.items()), flush=True)
            stats.report()
    stats.report()


# ── job lists ─────────────────────────────────────────────────────────────────
def fmt_uid(u):
    """The SHAB XML sometimes has the bare form CHE123456789; the registers want CHE-123.456.789."""
    d = re.sub(r"\D", "", u or "")
    return f"CHE-{d[0:3]}.{d[3:6]}.{d[6:9]}" if len(d) == 9 else u


def hosts_for(canton, lng=None):
    return vs_hosts(lng) if canton == "VS" else [CANTON_HOST[canton]]


def shab_frames(shab, geo):
    e = (pl.read_ndjson(shab, infer_schema_length=10000).select("kind", "uid", "date", "pub", "cantons", "id")
         .with_columns(pl.col("uid").map_elements(fmt_uid, return_dtype=pl.String)))
    g = pl.read_parquet(geo, columns=["id", "lng"])
    return e.join(g, on="id", how="left")


def jobs_p1(shab, geo):
    e = shab_frames(shab, geo)
    opened = e.filter(pl.col("kind") == "open").select("uid").unique()
    cl = (e.filter(pl.col("kind") == "close").sort("date").unique("uid", keep="last")
          .join(opened, on="uid", how="anti"))
    return [{"uid": r["uid"], "hosts": hosts_for(r["cantons"], r["lng"]), "deleted_tr": r["date"]}
            for r in cl.iter_rows(named=True)]


def jobs_sample(shab, geo, seed=7):
    """C0: 10 chregister active (opened 2018+, never closed: the HR01 journalDate
    checks the result), 10 chregister closed with no known opening, and for each of
    GE, VD, FR, NE 3 active + 2 closed."""
    e = shab_frames(shab, geo)
    closed_uids = e.filter(pl.col("kind") == "close").select("uid").unique()
    opens = e.filter(pl.col("kind") == "open").join(closed_uids, on="uid", how="anti").unique("uid")
    p1 = (e.filter(pl.col("kind") == "close").unique("uid")
          .join(e.filter(pl.col("kind") == "open").select("uid").unique(), on="uid", how="anti"))
    romand = ["GE", "VD", "FR", "NE"]
    pick = []
    a = opens.filter(~pl.col("cantons").is_in(romand)).sample(10, seed=seed)
    pick += [{"uid": r["uid"], "hosts": hosts_for(r["cantons"], r["lng"]), "sample": "chreg_active"}
             for r in a.iter_rows(named=True)]
    c = p1.filter(~pl.col("cantons").is_in(romand)).sample(10, seed=seed)
    pick += [{"uid": r["uid"], "hosts": hosts_for(r["cantons"], r["lng"]), "deleted_tr": r["date"],
              "sample": "chreg_closed"} for r in c.iter_rows(named=True)]
    for k in romand:
        pick += [{"uid": r["uid"], "hosts": [CANTON_HOST[k]], "sample": f"{k}_active"}
                 for r in opens.filter(pl.col("cantons") == k).sample(3, seed=seed).iter_rows(named=True)]
        pick += [{"uid": r["uid"], "hosts": [CANTON_HOST[k]], "sample": f"{k}_closed"}
                 for r in p1.filter(pl.col("cantons") == k).sample(2, seed=seed).iter_rows(named=True)]
    # old firms, to see real entry dates before the 1995 migration
    pick += [{"uid": "CHE-105.909.036", "hosts": ["zg.chregister.ch"], "sample": "old"},
             {"uid": "CHE-101.329.561", "hosts": ["zh.chregister.ch"], "sample": "old"},
             {"uid": "CHE-105.909.036", "hosts": ["prestations.vd.ch"], "sample": "old"}]
    return pick


# ── parquet ───────────────────────────────────────────────────────────────────
def to_parquet():
    rows = []
    for f in glob.glob(os.path.join(CACHE, "*", "*.json")):
        r = json.load(open(f))
        rows.append({
            "uid": r["uid"], "host": r["host"], "status": r["status"], "name": r.get("name"),
            "legal_form": r.get("legal_form"), "registered": r.get("registered"), "deleted": r.get("deleted"),
            "migrated": r.get("migrated"), "n_names": len(r.get("names") or []),
            "n_seats": len(r.get("seats") or []), "n_addresses": len(r.get("addresses") or []),
            "n_journal": len(r.get("journal") or []), "n_persons": r.get("n_persons"),
            "first_journal": min((j["tr_date"] for j in r.get("journal") or [] if j.get("tr_date")), default=None),
            "names": json.dumps(r.get("names") or [], ensure_ascii=False),
            "seats": json.dumps(r.get("seats") or [], ensure_ascii=False),
            "addresses": json.dumps(r.get("addresses") or [], ensure_ascii=False),
            "capital": json.dumps(r.get("capital") or [], ensure_ascii=False),
            "journal": json.dumps(r.get("journal") or [], ensure_ascii=False),
        })
    df = pl.DataFrame(rows, infer_schema_length=None).with_columns(pl.col("registered", "deleted", "migrated", "first_journal").cast(pl.String)
                                         .str.to_date(strict=False))
    df.write_parquet(os.path.join(BUILD, "cantonal.parquet"))
    print(df.group_by("host", "status").len().sort("host"))
    return df


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--jobs", choices=["p1", "sample"])
    p.add_argument("--jobs-file", help="run a job list written by --split")
    p.add_argument("--split", type=int, help="write the remaining p1 jobs as N files build/p1_part{i}.json: "
                   "chregister split by uid, every romand job in part 0 (one lane per romand host)")
    p.add_argument("--limit", type=int)
    p.add_argument("--lanes", type=int, default=CHREG_LANES, help="parallel lanes on the chregister server")
    p.add_argument("--parquet", action="store_true")
    p.add_argument("--shab", default=os.path.expanduser("~/Projetos/swissviz-timeline/build/shab_events.jsonl"))
    p.add_argument("--geo", default=os.path.expanduser("~/Projetos/swissviz-timeline/build/timeline_geo.parquet"))
    a = p.parse_args()
    CHREG_LANES = a.lanes
    os.makedirs(CACHE, exist_ok=True)
    if a.split:
        jobs = [j for j in jobs_p1(a.shab, a.geo) if not done(j)]
        parts = [[] for _ in range(a.split)]
        for j in jobs:
            k = 0 if j["hosts"][0] in HRC_BASE else int(re.sub(r"\D", "", j["uid"])) % a.split
            parts[k].append(j)
        for k, part in enumerate(parts):
            json.dump(part, open(os.path.join(BUILD, f"p1_part{k}.json"), "w"))
            print(f"part {k}: {len(part)} jobs")
    elif a.jobs_file:
        run(json.load(open(a.jobs_file)))
    elif a.jobs:
        jobs = jobs_p1(a.shab, a.geo) if a.jobs == "p1" else jobs_sample(a.shab, a.geo)
        if a.jobs == "sample":
            json.dump(jobs, open(os.path.join(BUILD, "cantonal_sample.json"), "w"), indent=0)
        if a.limit:
            jobs = jobs[: a.limit]
        run(jobs)
    if a.parquet:
        to_parquet()
