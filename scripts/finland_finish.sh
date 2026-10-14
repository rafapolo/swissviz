#!/bin/bash
# Runs on finland (~/cantonal/finish.sh) so the end of the collection needs no Mac:
#  - every 2 min, each DigitalOcean droplet tagged "cantonal" with no collector in two checks in a row
#    gets its cache copied to finland and is deleted through the API;
#  - when no droplet is left and livre's chregister worker is done, livre's cache is copied too.
# Token: ~/cantonal/.do_token (mode 600, never printed). Key: ~/.ssh/cantonal_do (finland's own).
# Log: ~/cantonal/build/finish.log. Exits when all of it is done; the Fribourg worker keeps running.
cd ~/cantonal
T=$(cat .do_token)
KEY="-i $HOME/.ssh/cantonal_do -o BatchMode=yes -o ConnectTimeout=10"
api() { curl -s -m 30 -H "Authorization: Bearer $T" "$@"; }
log() { echo "$(date -u +%H:%MZ) $*" >> build/finish.log; }
busy() { ssh -n $KEY "$1" 'ps -eo args | grep -c "[f]etch_cantonal.py --jobs"; true' 2>/dev/null; }
mkdir -p build/.idle
log "start"
while true; do
  rows=$(api "https://api.digitalocean.com/v2/droplets?tag_name=cantonal&per_page=200" | python3 -c "
import json,sys
for d in json.load(sys.stdin)['droplets']:
    ip=[n['ip_address'] for n in d['networks']['v4'] if n['type']=='public'][0]
    print(d['id'], d['name'], ip)" 2>/dev/null) || { log "API unreachable"; sleep 120; continue; }
  while read -r id name ip; do
    [ -n "$id" ] || continue
    n=$(busy "root@$ip"); [ -n "$n" ] || continue
    if [ "$n" != 0 ]; then echo 0 > "build/.idle/$name"; continue; fi
    k=$(( $(cat "build/.idle/$name" 2>/dev/null || echo 0) + 1 )); echo $k > "build/.idle/$name"
    [ "$k" -ge 2 ] || continue
    rsync -a -e "ssh $KEY" "root@$ip:cantonal/build/cantonal/" build/cantonal/ || { log "$name rsync failed"; continue; }
    rsync -a -e "ssh $KEY" "root@$ip:cantonal/build/cantonal_chf.log" "build/cantonal_chf_$name.log" 2>/dev/null
    code=$(api -o /dev/null -w "%{http_code}" -X DELETE "https://api.digitalocean.com/v2/droplets/$id")
    log "$name done: cache copied, delete -> HTTP $code"
  done <<< "$rows"
  if [ -z "$rows" ]; then
    n=$(busy ubuntu@83.228.221.143)
    if [ "$n" = 0 ]; then
      rsync -a -e "ssh $KEY" ubuntu@83.228.221.143:cantonal/build/cantonal/ build/cantonal/ && {
        log "no droplet left, livre done, its cache copied: $(find build/cantonal -name '*.json' | wc -l) excerpts here"
        exit 0; }
    fi
  fi
  sleep 120
done
