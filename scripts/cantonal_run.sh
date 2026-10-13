#!/bin/bash
# Remote worker control, installed as ~/cantonal/run.sh.
# usage: run.sh <jobs> <lanes> <note> [extra flags]   starts build/p1_<jobs>.json, log build/cantonal_<jobs>.log
#        run.sh stop [<jobs>]           stops that worker, or every worker
# Stops only the worker of the same jobs file, so a chregister worker and a romand worker can coexist.
cd ~/cantonal
pat="scripts/fetch_cantonal.py --jobs-file build/p1_${2:+$1}"
[ "$1" = stop ] && pat="scripts/fetch_cantonal.py --jobs-file build/p1_$2"
for p in $(pgrep -f "$pat"); do kill $p; done
sleep 2
[ "$1" = stop ] && { echo stopped; exit 0; }
log=build/cantonal_$1.log
echo "=== restart $(date +%H:%M) p1_$1.json, --lanes $2 $4 $3" >> $log
nohup python3 -u scripts/fetch_cantonal.py --jobs-file build/p1_$1.json --lanes $2 $4 >> $log 2>&1 < /dev/null &
sleep 4; tail -1 $log; ps -eo args | grep -c "[f]etch_cantonal.py --jobs-file"
