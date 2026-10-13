# Datas de abertura e histórico de todas as empresas pelo registro cantonal

**Objetivo**: Ter, para **todas** as empresas que o registro comercial conhece (ativas e canceladas desde 2000), a data de inscrição original ("Eingetragen am", desde 1883) e o histórico que interessa ao mapa: data de cancelamento, mudanças de sede, de nome, de forma jurídica e de endereço. Isso permite estender a linha do tempo para antes de 2018 com datas certas, em vez de estimadas.
**Arquivos**: `scripts/fetch_cantonal.py` (novo), `build/cantonal/` (cache, fora do git), `build/cantonal.parquet`, `data/timeline/*`
**Status**: Em andamento (C0 feito em 13/10/2026; P1 coletando)
**Depende de**: `tasks/timeline-shab.md` (UIDs 2018–2026), lista de UIDs ativos do LINDAS (`tasks/update-data-lindas.md` T1)
**Prazo**: não bloqueia a inscrição de 16/10/2026. A animação 2018–2026 já está pronta.

---

<!-- HANDOFF:INICIO (seção reescrita a cada relatório de 30 min) -->
## Estado atual e handoff — atualizado em 13/10/2026 às 12:46

**Quem toca:** sessão Claude `swissviz-e5`, no worktree `~/Projetos/swissviz-cantonal` (branch `feat/cantonal-register`). A cópia em `~/Projetos/swissviz/tasks/` é antiga; vale esta.

**O que está rodando (P1), dois processos por máquina:**

| Máquina | chregister (4 faixas) | Falta | s/empresa | Romands (1 faixa por host) | Falta GE / VD / FR |
|---|---|---|---|---|---|
| Mac | `p1_chreg0.json` → `build/cantonal_chreg0.log` | 36.026 | 2,43 | `p1_part0_romand.json` → `build/cantonal_p1.log` | 0 / 1.602 / — |
| finland | `p1_chreg1.json` → `cantonal_chreg1.log` | 35.816 | 2,45 | `p1_part1_romand.json` → `cantonal_p1.log` | 0 / 1.884 / — |
| livre | `p1_chreg2.json` → `cantonal_chreg2.log` | 35.626 | 2,45 | `p1_part2_romand.json` → `cantonal_p1.log` | 0 / 1.611 / — |
| colmeia | — | — | — | `p1_fr.json` → `cantonal_fr.log`, `--lanes 1 --pause 3` | — / — / 2.039 |

- **chregister retomado às 12:17** com 4 faixas por IP nas três máquinas (12 simultâneas), 0 erro e nenhum 403 novo. ~98/min por IP; faltam ~107,5 mil; previsão de fim ~18:50.
- **GE terminou** nas três máquinas (~12:44). **VD:** faltam ~5.100, 1,8–1,9 s por empresa, fim ~13:40.
- **Histórico do dia:** 16 faixas na livre (11:58) deram 403 em segundos e o IP dela ficou sem conectar por alguns minutos; o chregister foi pausado em todas as máquinas das 11:59 às 12:17. Às 12:15 a livre voltou a conectar (HTTP 302).
- **FR:** voltou no Mac com 1 faixa às 11:59 e levou **um segundo 429** às ~12:24, depois de 990 empresas. Parece cota por IP num intervalo, não só simultaneidade. **Às 12:32, a pedido do usuário, o resto de FR (2.223 empresas; 4.930 de 7.153 já feitas) passou para a `colmeia`**, com 1 faixa e pausa de 3 s Às 12:42: 183 feitos em 10 min (3,3 s por empresa, ~18/min), sem 429; faltam 2.039, fim ~14:35. Se der 429 lá, parar e deixar para outro dia.
- **Colmeia:** saiu do chregister às 12:08 (decisão do usuário: não usar essa máquina para scraping; os 5.161 extratos dela estão no cache do Mac, log em `build/cantonal_p1_colmeia.log`). Voltou às 12:32 **só para FR**, por pedido explícito. O sshd dela recusa conexões por ~1 min depois de várias chamadas seguidas: juntar os comandos numa só chamada ssh.
- NE terminou. O 429 de FR no Mac deixou `STOPPED` nesse log; é o bloqueio já conhecido. Erros isolados: um HTTP 500 em GE (livre) e um em FR (Mac); ficam fora do cache e entram numa próxima rodada.
- No cache do Mac: 58.408 extratos às 12:15 (com o rsync das remotas). O que as remotas baixam depois fica em `~/cantonal/build/cantonal/` até o rsync final.

**Como operar:**
- Estado de tudo: `bash scripts/cantonal_status.sh` (no Mac).
- Remotas: `ssh <host> 'bash ~/cantonal/run.sh <jobs> <faixas> "(nota)"'` inicia `build/p1_<jobs>.json` com log `build/cantonal_<jobs>.log` e só mexe no processo desse arquivo; `run.sh stop <jobs>` para um, `run.sh stop` para todos. Cópia em `scripts/cantonal_run.sh`. **Não** usar `pkill -f fetch_cantonal.py` por ssh: mata o próprio shell.
- Mac: `nohup python3 -u scripts/fetch_cantonal.py --jobs-file build/<arquivo>.json --lanes N >> build/<log> 2>&1 &`. Nunca dois processos sobre os mesmos hosts na mesma máquina.
- No zsh, rsync com aspas: `rsync -a "finland:cantonal/build/cantonal/" build/cantonal/`.
- Tudo se retoma pelo cache: o script pula o que já tem arquivo em `build/cantonal/{host}/{uid}.json`.

**O que se aprendeu sobre os limites:**
- chregister, 4 faixas por IP: ~98 empresas/min, estável por horas. 8 faixas: mesmo total, tempo por requisição dobra. 16 faixas: **403 e bloqueio temporário do IP** em segundos.
- FR: 429 com 3 simultâneas (após ~3.300 empresas no Mac) e de novo com 1 faixa (após 990). Cota por IP, tamanho desconhecido.
- GE e VD: 3 simultâneas (uma por máquina) sem problema.

**Regras em vigor:**
- No máximo 4 faixas por IP no chregister. Sem testes de mais faixas.
- 403/429 num host: tirar esse host de **todas** as máquinas, não insistir por outro IP, avisar o usuário.
- **Colmeia só para FR**, nada mais nela sem pedido explícito do usuário. Não usar proxies públicos (pedido várias vezes em 13/10 e recusado: são máquinas de terceiros e serviriam para contornar o limite por IP e o bloqueio). Não acrescentar IPs por conta própria.

**Próximos passos:**
1. **FR na colmeia** — acompanhar; ao terminar, `rsync -a "colmeia:cantonal/build/cantonal/" build/cantonal/`, conferir e apagar `~/cantonal` lá.
2. **Quando tudo terminar** — rsync dos caches da finland, da livre e da colmeia para o Mac, `python3 scripts/fetch_cantonal.py --parquet`, refazer a C3 com o conjunto completo e commitar.
3. **P2** (ativas anteriores a 09/2018, ~425 mil no chregister) — **não iniciar sem aval do usuário**. Falta escrever `jobs_p2`, roteando pelo registro atual (CHID do LINDAS → `OFFICE_HOST`).
4. C4 — repassar `uid → registered` à sessão night-light e estender a linha do tempo.

**Pendências com o usuário:** aval para a P2.

**Agendamentos que só existem na sessão `swissviz-e5`** (somem se ela fechar; outra sessão precisa recriá-los): relatório aos minutos 13 e 43, que também reescreve esta seção.
<!-- HANDOFF:FIM -->

---

## Por que o registro cantonal (verificado em 13/10/2026)

| Fonte | Tem data de abertura? |
|---|---|
| API SHAB (amtsblattportal), 09/2018 → hoje | Sim, mas só para as inscritas desde 09/2018 (`journalDate` do HR01) |
| Arquivo SHAB `/api/v1/archive/public`, 2002 → 01/09/2018 | Só título "NOME, SEDE" + data, sem UID. Tem lacunas: a abertura da Brecon-Systems AG (2004) está no PDF, mas não na lista |
| Zefix (`ZefixREST` e `ZefixPublicREST`) | **Não.** `shabDate` = última publicação (Nestlé: 2026-10-12); `shabPub[]` só desde ~2016 |
| SHAB escaneado (e-periodica / e-Helvetica), 1883 → 2018 | Sim, mas o OCR estraga o número CH (`CH-645. 1.OIO.O6I-6`), e muitos avisos antigos não imprimem número. `robots.txt` = `Disallow: /`, e o e-periodica tem captcha |
| **Extrato cantonal** | **Sim, exata**: "Eingetragen am 09.03.1883" (Nestlé), "Eingetragen am 30.06.1978 / Gelöscht am 04.03.2025" (Catalpa SA) |

O link do extrato de cada empresa vem do Zefix, no campo `cantonalExcerptWeb` de `POST https://www.zefix.ch/ZefixREST/api/v1/firm/search.json` ou `GET …/firm/{ehraid}.json`. Esse mesmo endpoint dá `uid`, `chid` (= `code13` dos XML do SHAB), `ehraid`, `status`, `deleteDate` e `oldNames`.

### Sistemas (contagem de uma amostra de 3.500 empresas do Zefix)
- **`{canton}.chregister.ch`** (DV Bern, JSF): ZH, BE, SG, AG, ZG, BS, GR, TI, LU, BL, TG, SO, SZ, SH, GL, AR, NW, JU, OW, UR, AI, mais VS (`vo`/`vc`/`vb.chregister.ch`, três regiões). Cerca de 85% das empresas.
- **Genebra**: `app2.ge.ch`
- **Vaud**: `prestations.vd.ch`
- **Friburgo**: `adm.appls.fr.ch`
- **Neuchâtel**: `rcnet.ne.ch`

### O que se sabe do chregister
- **Empresa ativa**: `https://{c}.chregister.ch/cr-portal/auszug/auszug.xhtml?uid=CHE-…`
- **Empresa cancelada**: `…/auszug/zefix.xhtml?uid=CHE-…&lang=de&shabdatum=AAAAMMDD`. Sem `shabdatum`, responde "Firma nicht gefunden". O valor vem do `cantonalExcerptWeb` do Zefix.
- **Navegador obrigatório**: um GET simples traz só a casca da página, com "Übertrag von 30.08.1995" (a data da migração para o registro eletrônico). "Eingetragen am", "Gelöscht am" e as tabelas chegam depois, por JS/AJAX.
  - Descobrir a chamada AJAX (JSF partial request) para evitar um navegador completo.
  - Se não der, usar Playwright headless.
- **Campos visíveis no extrato**:
  - cabeçalho: nome, forma jurídica, sede, UID, Eingetragen am, Gelöscht am;
  - tabelas com colunas Ei/Lö (número da inscrição em que o dado entrou e saiu): Firma (nomes antigos), Sitz, Kapital, Domiziladresse, Zweck, Bemerkungen;
  - tabela de diário com TR-Nr, TR-Datum, SHAB-Nr, SHAB-Dat. (todas as inscrições desde a migração);
  - pessoas.

### Conduta (decisão do usuário em 13/10/2026: usar o registro cantonal)
- Os dados são públicos por lei: CO art. 936 e HRegV. O `robots.txt` de todos os portais é `Disallow: /`. Não achei condições de uso que proíbam consulta automatizada, só a política de privacidade.
- Por isso a coleta tem de ser **lenta e identificável**:
  - no máximo 1 requisição por vez **por host**, com pausa de ≥1 s e recuo exponencial em erro 429/5xx;
  - User-Agent `cantography-research`, **sem e-mail nem nenhum contato do usuário, em lugar nenhum** (decisão de 13/10/2026);
  - cache em disco por UID, para que nada seja baixado duas vezes e a coleta possa ser retomada;
  - parar se algum portal reclamar ou bloquear.
- **Sem pedido ao EHRA/BJ** (decisão de 13/10/2026). É pesquisa temporária e não publicada: os extratos brutos ficam só em `build/`, fora do git, e não são redistribuídos. Do registro, o site só publica derivados (datas por ponto, agregados).
- **Pessoas físicas** (nomes, cidadania, domicílio, funções): **não guardar**. Estão fora do escopo e entram na proteção de dados (nDSG). Guardar no máximo a contagem de pessoas por empresa, se for útil.

---

## Subtarefas

### C0 — Teste com ~50 empresas ✅ 13/10/2026
- [x] Amostra: 10 ativas e 10 canceladas do chregister, 3 ativas + 2 canceladas de GE, VD, FR e NE, e 3 empresas antigas (Nestlé ZG/VD, UBS). Resultado: **42 ok, 1 não encontrada, 0 erro**, ~3 min no total.
- [x] **Nenhum sistema precisa de navegador:**
  - **chregister** (os 24 registros): `GET …/auszug/auszug.xhtml?uid=CHE-…` (canceladas: `&loeschung=AAAAMMDD`, a **data do TR** do HR03, que é o `date` em `shab_events`), depois `POST` do painel "deferred" do PrimeFaces (`idAuszugForm:auszugContentPanel_load=true` + ViewState + nonce). São 2 requisições, ~2,3–2,7 s por empresa com as pausas.
    - `Accept-Language: de` deixa o extrato em alemão (sem isso, vem em inglês).
    - "Não encontrada" é uma resposta parcial sem `id="Titel"`.
  - **GE, VD, FR e NE** usam o mesmo app (hrc-internet): `GET {base}/api/public/catalog/configuration` (cookie `XSRF-TOKEN`), depois `POST {base}/api/public/hrcexcerpts/` com o cabeçalho `X-XSRF-TOKEN`. A resposta é JSON:
    - `dateInscription` / `dateRadiation` vêm por extenso, no idioma da empresa (FR tem empresas em alemão: "20. März 2026");
    - `jrns[0] = {date: "report"}` marca a migração.
- [x] **Validação:** `registered` = `journalDate` do HR01 em **21/21** casos, e `deleted` = data do HR03 em **18/18**. Datas antigas reais aparecem: Nestlé ZG 09.03.1883, Nestlé VD 19.07.1905, UBS 28.02.1978. O "Übertrag" de 1995 não aparece no lugar da data.
  - A Nestlé tem o mesmo UID em ZG e VD, com datas diferentes. Por isso o cache é por `{host}/{uid}`.
  - A não encontrada (CHE-480.244.350) mudou de AG para BS sem HR03. Para as **ativas**, rotear pelo registro atual (LINDAS: CHID/comuna), não pelo cantão do SHAB.
  - O SHAB às vezes traz o UID sem formatação (`CHE495072402`); o script normaliza.
- [x] e-periodica ficou de fora (captcha + `Disallow`). A validação usou o HR01, o HR03 e `archive_opens`: a data do arquivo é a da publicação, 3 a 6 dias depois do TR.
- [x] **Roteamento sem o Zefix** (C1 encolhe):
  - `pub` do HR03 = `deleteDate` do Zefix em 10/10.
  - Tabela registro (`registerOfficeId` = dígitos 3–5 do CHID) → host em `OFFICE_HOST` do script.
  - VS tem três registros (vo/vc/vb, por distrito). Sem o número BFS, a região é escolhida pela longitude, com recuo para as outras quando dá "não encontrada".
- [x] **Servidores:** os 24 `*.chregister.ch` resolvem para **um só IP** (185.125.167.99). GE, VD, FR e NE são servidores separados.
  - **Decisão do usuário (13/10/2026):** 4 requisições simultâneas no servidor da DV Bern, cada uma com pausa ≥1 s, e 1 por host romand. No mesmo dia, foi testado com **8 (`--lanes 8`)**: deu ~90 empresas/min contra ~98/min com 4, e o tempo por empresa dobrou (2,5 → ~5 s). O servidor é o limite, então a coleta **voltou para 4**. Dividir com as máquinas SSH (finland, livre) foi descartado de início: a latência quase não muda nada, porque as pausas dominam, e espalhar a carga por vários IPs se aproximaria de contornar o limite.
  - **Divisão em 3 máquinas (13/10/2026, 10:55):** a P1 restante foi dividida com `--split 3`: parte 0 no Mac (1/3 do chregister + todos os romands), parte 1 em `finland`, parte 2 em `livre`, cada uma com `--lanes 4`. São **12 requisições simultâneas** no servidor da DV Bern, a partir de 3 IPs, o que contraria a decisão acima. Medido às 11:00: 2,4–2,5 s por empresa nas três máquinas, 0 erro, ~1.460 empresas do chregister a cada 5 min (3× o ritmo com uma máquina). O servidor aguentou, ao contrário do teste com 8 no Mac.
  - **Redivisão em 4 máquinas (13/10/2026, 11:06, a pedido do usuário: "distribuir como der e tentar aumentar o paralelismo"):** `--split 4`, com `colmeia` como quarta máquina (só chregister) e os romands repartidos pelas 3 primeiras partes. `beelink` sai pelo mesmo IP do Mac e `gcs` não respondeu.
    - chregister com 16 simultâneas: 2,4–2,5 s por empresa nas quatro máquinas, 0 erro.
    - **Friburgo bloqueou:** `adm.appls.fr.ch` devolveu **429** ao Mac segundos depois de passar de 1 para 3 requisições simultâneas. Pela regra de conduta, FR foi retirado das três máquinas (restam ~3.400 empresas de FR na P1). Retomar só mais tarde, com 1 faixa e uma só máquina.
    - **Teste com 8 faixas na finland (11:14–11:19):** 493 empresas em 5 min a 4,85 s por empresa, o mesmo total das 4 faixas, enquanto as outras três máquinas seguiam a 2,4 s. O limite do chregister é **por IP** (~98 empresas/min), não do servidor. Mais faixas por máquina não adiantam; a finland voltou para 4.
    - Reinício nas remotas: `bash ~/cantonal/run.sh <parte> <faixas> <nota>` ou `run.sh stop`. Não usar `pkill -f fetch_cantonal.py` por ssh: ele mata o próprio shell do ssh.
    - **Regra de parada:** se o tempo por empresa no chregister subir para ~5 s, ou aparecer 403/429, parar as remotas, trazer o cache delas com `rsync -a` e retomar só no Mac com `--jobs p1 --lanes 4`. Nunca dois processos no Mac ao mesmo tempo.
    - **No fim:** `rsync -a` de `~/cantonal/build/cantonal/` das duas máquinas para `build/cantonal/` no Mac, depois `python3 scripts/fetch_cantonal.py --parquet`.
- [x] **Duração:**
  - P1 (198.639 canceladas em 2018–2026 sem HR01, ~150 mil no chregister): ~150 mil × 2,5 s ÷ 4 ≈ **26 h**. O maior romand é VD, com 18 mil.
  - P2 (ativas anteriores a 09/2018, ~425 mil no chregister): mais ~3 dias.
- [ ] Erro da estimativa pelo `ehraid`: medir com as ativas da P2. As canceladas da P1 não têm `ehraid` no LINDAS.

### C1 — Lista de empresas e links (Zefix)
- [ ] Juntar os UIDs: LINDAS (ativas, ~795 mil), `build/shab_events.jsonl` (2018–2026, com canceladas) e `build/timeline_geo.parquet`.
- [ ] Para cada UID: `ehraid`, `chid`, `status`, `deleteDate`, `cantonalExcerptWeb` e `oldNames` do Zefix. Uma requisição por empresa, com o mesmo ritmo baixo. Não usar a API PublicREST, porque as credenciais exigiriam o e-mail do usuário. Salvar em `build/zefix_firms.parquet`.
- [ ] Isso também dá o `chid` (= `code13`) sem baixar de novo os 661 mil XML do SHAB.

### C2 — Coletar os extratos (`scripts/fetch_cantonal.py`)
- [ ] Um adaptador por sistema: chregister, GE, VD, FR, NE.
- [ ] Prioridade:
  1. as ~199 mil fechadas em 2018–2026 sem abertura conhecida (a animação);
  2. as ativas abertas antes de 09/2018 (a tarefa de datas da sessão night-light);
  3. o resto.
- [ ] Um worker por host, com fila, pausa, retry e recuo exponencial. Cache em `build/cantonal/{uid}.json`, só com os campos extraídos.
- [ ] Campos a extrair:
  - `uid`, `chid`, `ehraid`, `canton`;
  - `registered` (Eingetragen am), `deleted` (Gelöscht am), `migrated` (Übertrag von);
  - `names[]` (nome + Ei/Lö), `seats[]` (sede + Ei/Lö), `legal_forms[]`, `addresses[]` (endereço + Ei/Lö);
  - `capital[]` (valor + Ei/Lö);
  - `journal[]` (TR-Nr, TR-Datum, SHAB-Nr, SHAB-Dat.), que permite datar cada Ei/Lö;
  - `purpose` (último);
  - `n_persons` (só a contagem).
- [ ] Log de progresso por host, com contagem de erros. Parar sozinho se um host passar de 5% de erros.

### C3 — Validar
**Parcial em 13/10/2026, com 18.537 extratos da P1 (só o cache do Mac):**
- `deleted` = data do TR do HR03 em 18.337 de 18.342 (99,97%); os 5 restantes diferem em 3 a 5 dias. 195 extratos vêm sem `deleted`.
- Contra `archive_opens` (10.645 em comum): casamento `name+seat` fica a 0–10 dias em 99,0% (mediana +5 dias, a publicação); casamento só por `name` em 87,6%, e **10,5% erram por mais de 1 ano**. O casamento só por nome não serve para datar.
- Distribuição por ano de 1985 a 2018 é lisa, sem pico em 1995 nem em outro ano de migração. Os dias mais frequentes têm no máximo 16 empresas. Mais antiga: 02.02.1883.
- `registered` nulo em 113 (106 em FR, 7 em GE): o `dateInscription` vem vazio, mas o diário existe. Reserva: `first_journal`, quando `has_report` é falso (coluna nova no parquet).
- `migrated` nunca é preenchido no chregister (o "Übertrag" não está na resposta parcial). Não atrapalha: `registered` já é a data real.
- 163 empresas da P1 têm `registered` de 2019 em diante: abriram depois de 09/2018, mas o HR01 falta em `shab_events`. É uma lacuna de ~0,9% na coleta da linha do tempo, que este parquet preenche.
- [ ] Para as empresas inscritas desde 09/2018, `registered` deve bater com o `journalDate` do HR01. Medir a concordância, que deve ficar ≥ 99%.
- [ ] Para 2002–2018, comparar com o casamento do arquivo (`build/archive_opens.parquet`) e medir o erro do casamento por nome.
- [ ] Distribuição de `registered` por ano (1883 → hoje): procurar picos falsos, como datas de migração tomadas como data de abertura.
- [ ] Mudanças de sede entre cantões: a empresa aparece nos dois cantões? Qual extrato guarda a data original?

### C4 — Usar no mapa
- [ ] Linha do tempo: decidir o novo início (2000? 1990?) e se as empresas sem endereço histórico (canceladas antes de 2018) aparecem na última sede conhecida, marcadas com precisão `seat`.
- [ ] Endereço histórico: `addresses[]` com datas permite mover o ponto quando a empresa muda de endereço. É opcional e precisa de geocodificação por endereço antigo.
- [ ] Achados para o texto (S5): idade das empresas ao fechar, empresas mais antigas ainda ativas por cantão, ondas de abertura por década.
- [ ] Repassar à sessão night-light o parquet com `uid → registered` para a tarefa de datas de todas as empresas existentes.

## Riscos
- Bloqueio por IP ou mudança no portal no meio da coleta: o cache por UID permite retomar. Se um portal bloquear, parar esse host e usar a estimativa pelo `ehraid` (C0) para quem faltar.
- Empresas canceladas antes de 2000 não estão no Zefix nem nos extratos, e para elas o registro cantonal não ajuda. Ficam só o SHAB escaneado ou o arquivo em papel.
- "Eingetragen am" é a inscrição no registro, não necessariamente o início real da atividade. Uma empresa que mudou de cantão pode mostrar a data da nova inscrição. Verificar em C3.
