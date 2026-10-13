#!/bin/bash
# Re-split every chregister job still to do (P2 first, then P1) over the Mac and the remote machines,
# 4 lanes each. Romand workers are left alone.
# usage: bash scripts/cantonal_rebalance.sh finland livre cantonal-1 ...
set -e
cd "$(dirname "$0")/.."
REMOTES=("$@")
touch build/.rebalancing   # scripts/do_reaper.sh waits while this exists
trap 'rm -f build/.rebalancing' EXIT
# 1. stop every chregister worker
for p in $(pgrep -f "fetch_cantonal.py --jobs-file build/(p1_chreg|p2_chreg|chreg)"); do kill $p; done
for h in "${REMOTES[@]}"; do
  ssh -o BatchMode=yes "$h" 'cd ~/cantonal && for p in $(pgrep -f "fetch_cantonal.py --jobs-file build/p1_(chreg|p2_chreg|do|ch)"); do kill $p; done; true'
done
sleep 3
# 2. merge the caches
for h in "${REMOTES[@]}"; do rsync -a "$h:cantonal/build/cantonal/" build/cantonal/; done
# 3. split: P2 before P1 in every part
n=$(( ${#REMOTES[@]} + 1 ))
python3 - "$n" <<'EOF'
import sys, json, re, glob
sys.path.insert(0, "scripts"); import fetch_cantonal as f
n = int(sys.argv[1])
seen, jobs = set(), []
for pat in ["build/p2_chreg[0-9].json", "build/p1_chreg[0-9].json"]:
    for path in sorted(glob.glob(pat)):
        for j in json.load(open(path)):
            if j["uid"] not in seen and not f.done(j):
                seen.add(j["uid"]); jobs.append(j)
parts = [[] for _ in range(n)]
for i, j in enumerate(jobs):
    parts[i % n].append(j)
for k, p in enumerate(parts):
    json.dump(p, open(f"build/ch_part{k}.json", "w"))
print(f"{len(jobs)} chregister jobs left, {len(jobs) // n} per machine")
EOF
# 4. start: part 0 on the Mac, part i on remote i
echo "=== restart $(date +%H:%M) ch_part0.json, --lanes 4 (rebalance over $n machines)" >> build/cantonal_ch.log
nohup python3 -u scripts/fetch_cantonal.py --jobs-file build/ch_part0.json --lanes 4 >> build/cantonal_ch.log 2>&1 < /dev/null &
k=1
for h in "${REMOTES[@]}"; do
  rsync -a scripts/fetch_cantonal.py "$h:cantonal/scripts/"
  rsync -a scripts/cantonal_run.sh "$h:cantonal/run.sh"
  rsync -az "build/ch_part$k.json" "$h:cantonal/build/p1_ch.json"
  ssh -o BatchMode=yes "$h" "bash ~/cantonal/run.sh ch 4 '(rebalance over $n machines, part $k)' | head -1"
  k=$((k + 1))
done
