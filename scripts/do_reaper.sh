#!/bin/bash
# Delete each DigitalOcean droplet (tag "cantonal") as soon as its collector is done:
# no fetch_cantonal worker on it in two checks in a row (and no rebalance running) -> copy its cache to the Mac, delete the droplet,
# drop its block from ~/.ssh/config. Checks every 2 minutes; exits when no droplet is left.
# The token is read from ~/Projetos/swissviz/.env (variable "digitalocean") and never printed.
cd "$(dirname "$0")/.."
T=$(sed -n 's/^digitalocean=//p' ~/Projetos/swissviz/.env | tr -d "\"' \r")
api() { curl -s -m 30 -H "Authorization: Bearer $T" "$@"; }
mkdir -p build/.idle
while true; do
  [ -e build/.rebalancing ] && { sleep 120; continue; }
  rows=$(api "https://api.digitalocean.com/v2/droplets?tag_name=cantonal&per_page=200" |
         python3 -c "import json,sys; [print(d['id'], d['name']) for d in json.load(sys.stdin)['droplets']]")
  [ -z "$rows" ] && { echo "$(date +%H:%M) no droplet left"; python3 scripts/do_budget.py; exit 0; }
  while read -r id name; do
    n=$(ssh -n -o ConnectTimeout=10 -o BatchMode=yes "$name" 'ps -eo args | grep -c "[f]etch_cantonal.py --jobs"; true' 2>/dev/null)
    [ -n "$n" ] || continue   # unreachable: try again next round
    if [ "$n" != 0 ]; then echo 0 > "build/.idle/$name"; continue; fi
    k=$(( $(cat "build/.idle/$name" 2>/dev/null || echo 0) + 1 )); echo $k > "build/.idle/$name"
    [ "$k" -ge 2 ] || continue
    rsync -a "$name:cantonal/build/cantonal/" build/cantonal/ || continue
    rsync -a "$name:cantonal/build/cantonal_ch.log" "build/cantonal_ch_$name.log" 2>/dev/null
    code=$(api -o /dev/null -w "%{http_code}" -X DELETE "https://api.digitalocean.com/v2/droplets/$id")
    echo "$(date +%H:%M) $name done: cache copied, delete -> HTTP $code"
    if [ "$code" = 204 ]; then
      python3 - "$name" <<'EOF'
import os, re, sys
p = os.path.expanduser("~/.ssh/config"); s = open(p).read()
s = re.sub(r"\n*Host %s\n(?:[ \t]+.*\n?)*" % re.escape(sys.argv[1]), "\n", s)
open(p, "w").write(s.rstrip("\n") + "\n")
EOF
    fi
  done <<< "$rows"
  sleep 120
done
