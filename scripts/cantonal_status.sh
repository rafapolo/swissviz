#!/bin/bash
# Último bloco de 5 min de cada coletor: soma do chregister, s/empresa, erros, linhas "!!".
summ() { awk -v m="$1" '
  /^-- /{blk=$0; n=0; s=0; e=0; nf=0}
  /chregister\.ch/{n+=$3; nf+=$5; e+=$7; s+=$3*$8; last=1}
  /^!!/{bang=bang "\n   " $0}
  END{printf "%-8s %s\n   chregister (acumulado desde o início do processo): ok %d nf %d err %d  %.2f s/empresa%s\n", m, blk, n, nf, e, (n?s/n:0), bang}'; }
tail -200 ~/Projetos/swissviz-cantonal/build/cantonal_p1.log | awk '/^=== restart/{buf=""} {buf=buf $0 "\n"} END{printf "%s", buf}' | awk '/^-- /{buf=""} {buf=buf $0 "\n"} END{printf "%s", buf}' | summ mac
pgrep -f fetch_cantonal >/dev/null && echo "   processo: vivo" || echo "   processo: PARADO"
for h in finland livre colmeia; do
  ssh -o ConnectTimeout=10 -o BatchMode=yes $h 'tail -200 ~/cantonal/build/cantonal_p1.log; echo "@@ $(pgrep -f fetch_cantonal.py >/dev/null && echo vivo || echo PARADO) erros=$(wc -l < ~/cantonal/build/cantonal_errors.jsonl)"' 2>&1 > /tmp/.cs_$h.$$
  grep -v '^@@' /tmp/.cs_$h.$$ | awk '/^-- /{buf=""} {buf=buf $0 "\n"} END{printf "%s", buf}' | summ $h
  grep '^@@' /tmp/.cs_$h.$$ | sed 's/^@@/   processo:/'; rm -f /tmp/.cs_$h.$$
done
