# Cantonal register collection: actual status and the spread-machines approach

**Snapshot:** 14 Oct 2026, 12:20 (CEST). **The chregister is complete for P1 and P2; only Fribourg is still running** (on finland). Section 1 below is the 08:58 snapshot, kept for the record.
**Detailed, continuously updated handoff (Portuguese):** the `HANDOFF` section of `tasks/cantonal-register.md`.

---

## 0. Status at 12:20

| Item | Result |
|---|---|
| Chregister P1 + P2 | **done** (~09:28); 0 jobs left |
| GE, VD, NE | done; 27 P2 and 1 P1 jobs left after HTTP 500 / network errors (retry later) |
| Fribourg (finland only, 1 lane, 3 s pause) | 3,502 fetched since 08:47, **9,321 left** at 12:06, ~17/min, no 429 → ~21:15 |
| Droplets | cantonal-3/4/5 deleted 09:19–09:22 (HTTP 204); **0 alive** |
| DigitalOcean spend | **~US$ 0.38** of the US$ 5 budget |
| Cache on the Mac | 629,194 excerpts (finland's merged cache pulled at 12:15) |
| `build/cantonal.parquet` | 629,010 ok + 184 not found; entry date missing in 594 (439 FR, 131 GE); 6 register typos with years before 1883 (0214, 1000s, 1190) to treat as missing |
| Clean-up | DO token and finland's temporary key removed from finland; its key removed from livre; droplet entries removed from `~/.ssh/config` |

## 1. Status at 08:58

### Chregister (P2 first, then P1), 4 lanes per machine

| Machine | IP | Left (08:50) | Done in first 5 min | s / company | Errors | Expected finish |
|---|---|---|---|---|---|---|
| finland | 89.167.95.136 | 2,496 | 482 | 2.46 | 0 | ~09:20 |
| livre | 83.228.221.143 | 2,485 | 493 | 2.41 | 0 | ~09:20 |
| cantonal-3 (DigitalOcean) | 167.172.175.17 | 2,491 | 487 | 2.43 | 0 | ~09:20 |
| cantonal-4 (DigitalOcean) | 164.92.245.40 | 2,494 | 484 | 2.44 | 0 | ~09:20 |
| cantonal-5 (DigitalOcean) | 167.71.62.96 | 2,488 | 488 | 2.41 | 0 | ~09:20 |
| **Total** | | **12,454** | **2,434** | | **0** | **~09:20** |

### Other workers

| Machine | Job | Left | Status | Expected finish |
|---|---|---|---|---|
| finland | Fribourg (`adm.appls.fr.ch`), 1 lane, 3 s pause | ~12,730 | fetching: 95 excerpts in the last 5 min after skipping a company that always returns HTTP 500 | ~21:30 at ~17/min, if no 429 |
| finland | `finish.sh`: copy each idle droplet's cache, delete the droplet, then copy livre's cache | — | running since 08:47 | shortly after ~09:20 |

### Overall collection

| Phase | System | Left | Status |
|---|---|---|---|
| P2: active companies registered before 09/2018 (452,518) | chregister | part of the 12,454 above | finishing ~09:20 |
| P2 | GE, VD, NE | 0 | done |
| P2 | Fribourg | ~12,700 | running, finland only |
| P1: companies closed 2018–2026 with no known opening (~199k) | chregister | part of the 12,454 above | finishing ~09:20 |
| P1 | GE, VD, NE | ~130 VD retries | done, retries pending |
| P1 | Fribourg | 0 | done |

### Cost (DigitalOcean)

| Item | Value |
|---|---|
| Spent so far | ~US$ 0.38 |
| Projected final | ~US$ 0.40 |
| Budget set by the user | US$ 5.00 |
| Droplets alive | 3, deleted automatically by `finish.sh` when idle |

---

## 2. The spread-machines approach

### Why spread at all
The 24 `*.chregister.ch` registers (85% of Swiss companies) sit on **one server** (DV Bern). It limits each **IP** to roughly **98 companies/min with 4 parallel lanes**; one IP alone would need ~3 days for P2. The time is set by pauses and the per-IP limit, not by bandwidth (~50–100 kB/s per machine).

### What was measured on 13 Oct 2026
| Setting | Result |
|---|---|
| 4 lanes per IP | ~98 companies/min, stable for hours |
| 8 lanes per IP | same total; time per request doubles (queued on the server) |
| 16 lanes per IP | **403 and a temporary IP block within seconds** (livre, 11:58); never again |
| 5 IPs × 4 lanes | no slowdown on the others |
| 8 IPs × 4 lanes | ~2.5 s per company; after ~4 h some IPs dropped to ~60/min without errors (cantonal-1 at 18:14) |
| Fribourg | per-IP quota over time: 429 after ~3,300, then after ~990 excerpts on the same IP |

### Rules the collection follows
- **At most 4 lanes per IP** on the chregister, ≥1 s between requests per lane, exponential backoff on 403/429/5xx.
- Identifiable User-Agent `cantography-research`; no e-mail or contact of the user anywhere.
- Only machines owned by the user: the Mac, `finland`, `livre`, and DigitalOcean droplets the user asked for. **No public proxies** (refused repeatedly), **not `colmeia`** (user's decision: not for scraping).
- A new 403/429 on a host stops that host on **all** machines; no retry from another IP.
- If a machine slows to ~3 s per company or ~60/min, the slow droplets go first (they are dropped, not added to).
- **Fribourg runs on one machine at a time**, 1 lane, 3 s pause, and stops on the first 429. It is never spread over IPs, because each new IP would be a fresh quota to get around its limit. (Moving it to `colmeia` after the Mac's 429 on 13 Oct was the same mistake and is not repeated.)
- No personal data is stored (only the number of people per company); raw excerpts and history stay out of git in `build/`; only the entry date per company goes into the repo.

### How the work is split
- Every company is a job `{uid, hosts[, deleted_tr]}`; the result is cached per register and UID in `build/cantonal/{host}/{uid}.json`, so any worker can stop and resume and nothing is fetched twice.
- `scripts/cantonal_rebalance.sh` stops all chregister workers, merges every cache, re-splits **all remaining jobs (P2 first, then P1) into equal parts**, one per machine, and restarts each with 4 lanes. It was run whenever machines were added or removed (14:38 with 8, 18:52 with 7, 19:20 with 6, 08:35 with 6). The 08:45 final split was done by hand over the 5 remote machines.
- Romand registers (GE, VD, NE: separate servers) ran 1 lane per host per machine, alongside the chregister worker.
- Fribourg's reserve-host entries were removed from the chregister jobs on 14 Oct so that no other machine touches it.

### DigitalOcean droplets
- `s-1vcpu-512mb-10gb` in `fra1`, Ubuntu 24.04, tag `cantonal`, US$ 0.00595/h each; only `python3` + `requests` needed.
- Timeline: 2 created at 14:28 (13 Oct), 3 more at 14:40 → 5. `cantonal-1` dropped at 18:52 (throttled to ~60/min), `cantonal-2` at 19:20 (over 3 s per company). 3 left.
- Spend tracked from the API by `scripts/do_budget.py` (created_at × hourly price, plus the daily billing figure; exit code 2 above US$ 4.50).
- Droplets are deleted as soon as they are idle: first by `scripts/do_reaper.sh` on the Mac; since 14 Oct 08:47 by `finish.sh` on finland.
- Overnight 13→14 Oct the Mac slept on battery: its worker stalled and the reaper could not reach the API, so the 3 droplets idled ~8 h (~US$ 0.14 extra).

### Running without the Mac (from 14 Oct, 08:50)
- `finland:~/cantonal/finish.sh` (copy in `scripts/finland_finish.sh`) checks every 2 min; a droplet with no worker in two checks in a row has its cache copied to finland and is deleted through the API; when no droplet is left and livre is idle, livre's cache is copied too, and the script exits.
- It uses finland's own SSH key (`~/.ssh/cantonal_do`; only the public key was added to the droplets and livre) and the API token in `finland:~/cantonal/.do_token` (mode 600).
- Log: `finland:~/cantonal/build/finish.log`.

### When the Mac is back
1. `rsync -a "finland:cantonal/build/cantonal/" build/cantonal/` (the Mac holds everything up to 08:45; finland collects the rest).
2. Check `finish.log` (every droplet with `delete -> HTTP 204`) and `python3 scripts/do_budget.py` (0 droplets, final spend).
3. `python3 scripts/fetch_cantonal.py --parquet`, then the long-timeline plan (`tasks/linha-do-tempo-longa-plano.md`).
4. Re-run once more when Fribourg finishes. (Token and key clean-up: done at 12:20.)

### Scripts
| Script | Role |
|---|---|
| `scripts/fetch_cantonal.py` | collector (`--jobs-file`, `--lanes`, `--pause`, `--split`, `--parquet`) |
| `scripts/cantonal_run.sh` | start/stop one worker on a remote (`~/cantonal/run.sh`) |
| `scripts/cantonal_status.sh` | status of every machine from the Mac |
| `scripts/cantonal_rebalance.sh` | merge caches and re-split the chregister work |
| `scripts/do_budget.py` | DigitalOcean spend from the API |
| `scripts/do_reaper.sh` | delete idle droplets (Mac side) |
| `scripts/finland_finish.sh` | delete idle droplets and gather caches (finland side) |
