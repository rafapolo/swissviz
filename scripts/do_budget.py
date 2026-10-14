#!/usr/bin/env python3
"""Spend of the DigitalOcean droplets tagged "cantonal", read from the API.

The billing balance (/customers/my/balance) is generated once a day, so it lags;
the running cost is computed from each droplet's created_at and its size's
hourly price, which is what DigitalOcean bills (per second, 60 s minimum,
capped at the monthly price). Deleted droplets drop out of the API, so their
cost is kept in build/do_spent.json when this script sees them disappear.

Budget set by the user on 2026-10-13: US$ 5 in all. Exit code 2 when the spend
so far is above STOP (the caller deletes the droplets).

Usage: python3 scripts/do_budget.py [--hours-left H]   # H: hours the droplets still need
The token is read from ~/Projetos/swissviz/.env (variable "digitalocean") and never printed.
"""
import argparse, datetime, json, os, sys, urllib.request

BUDGET, STOP = 5.0, 4.5
ENV = os.path.expanduser("~/Projetos/swissviz/.env")
LEDGER = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "build", "do_spent.json")


def token():
    for line in open(ENV):
        if line.startswith("digitalocean="):
            return line.split("=", 1)[1].strip().strip("'\"")
    sys.exit("no digitalocean token in " + ENV)


def api(path, tok):
    req = urllib.request.Request("https://api.digitalocean.com/v2/" + path, headers={"Authorization": "Bearer " + tok})
    return json.load(urllib.request.urlopen(req, timeout=30))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--hours-left", type=float, default=0)
    a = p.parse_args()
    tok = token()
    now = datetime.datetime.now(datetime.timezone.utc)
    ledger = json.load(open(LEDGER)) if os.path.exists(LEDGER) else {"live": {}, "gone": 0.0}
    live, rate = {}, 0.0
    for d in api("droplets?tag_name=cantonal&per_page=200", tok)["droplets"]:
        t0 = datetime.datetime.fromisoformat(d["created_at"].replace("Z", "+00:00"))
        hours = max(60, (now - t0).total_seconds()) / 3600
        cost = min(d["size"]["price_hourly"] * hours, d["size"]["price_monthly"])
        live[str(d["id"])] = round(cost, 5)
        rate += d["size"]["price_hourly"]
        print(f"  {d['name']:12} {d['status']:7} {hours:6.2f} h  US$ {cost:.4f}")
    for did, cost in ledger["live"].items():   # deleted since the last run
        if did not in live:
            ledger["gone"] += cost
    ledger["live"] = live
    os.makedirs(os.path.dirname(LEDGER), exist_ok=True)
    json.dump(ledger, open(LEDGER, "w"))
    spent = ledger["gone"] + sum(live.values())
    proj = spent + rate * a.hours_left
    bal = api("customers/my/balance", tok)
    print(f"droplets: {len(live)}  ritmo US$ {rate:.4f}/h  gasto até agora US$ {spent:.4f}  "
          f"projeção ao fim US$ {proj:.4f}  orçamento US$ {BUDGET:.2f}")
    print(f"faturamento DO (gerado {bal.get('generated_at')}): uso do mês US$ {bal.get('month_to_date_usage')}")
    sys.exit(2 if spent > STOP else 0)


main()
