#!/bin/bash
# Last 5-minute block of every worker log on the Mac, finland and livre:
# chregister total and s/firm, each romand host, "!!" lines, live workers.
last='for f in build/cantonal_*.log; do case $f in *errors*|*colmeia*) continue;; esac
  [ -n "$(find "$f" -mmin -30)" ] || continue
  echo "@log $f"; tail -120 "$f" | awk "/^-- /{buf=\"\"} {buf=buf \$0 \"\n\"} END{printf \"%s\", buf}"; grep "^!!" "$f" | tail -3; done
echo "@live $(ps -eo args | grep -c "[f]etch_cantonal.py --jobs")  errors=$(grep -c . build/cantonal_errors.jsonl)"'
summ() { awk -v m="$1" '
  /^@log/{if(f)flush(); f=$2; blk=""; n=nf=e=s=0; rom=""; next}
  /^@live/{if(f)flush(); f=""; printf "   workers vivos: %s  %s\n", $2, $3; next}
  /^-- /{blk=$0; next}
  /^!!/{rom=rom "\n     " $0; next}
  /chregister\.ch/{n+=$3; nf+=$5; e+=$7; s+=$3*$8; next}
  /^  [a-z0-9.]+ +ok/{rom=rom sprintf("\n     %-18s ok %s err %s  %s s/empresa%s", $1, $3, $7, $8, ($0~/STOPPED/?" STOPPED":"")); next}
  function flush(){printf "%-8s %s  %s\n", m, f, blk; if(n) printf "     chregister (desde o início do processo): ok %d nf %d err %d  %.2f s/empresa\n", n, nf, e, s/n; if(rom) print substr(rom,2)}
  END{if(f)flush()}'; }
(cd ~/Projetos/swissviz-cantonal && bash -c "$last") | summ mac
for h in finland livre cantonal-1 cantonal-2 cantonal-3 cantonal-4 cantonal-5; do ssh -o ConnectTimeout=10 -o BatchMode=yes $h "cd ~/cantonal && $last" 2>&1 | summ $h; done
