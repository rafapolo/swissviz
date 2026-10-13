#!/bin/bash
# usage: run.sh <part> <lanes> <note> | run.sh stop
cd ~/cantonal
for p in $(pgrep -f 'scripts/fetch_cantonal.py'); do kill $p; done
sleep 2
[ "$1" = stop ] && { echo stopped; exit 0; }
echo "=== restart $(date +%H:%M) part$1 of 4, --lanes $2 $3" >> build/cantonal_p1.log
nohup python3 -u scripts/fetch_cantonal.py --jobs-file build/p1_part$1.json --lanes $2 >> build/cantonal_p1.log 2>&1 < /dev/null &
sleep 4; tail -1 build/cantonal_p1.log; pgrep -f 'scripts/fetch_cantonal.py' | wc -l
