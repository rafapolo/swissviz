# Linha do tempo longa (1883 → hoje): plano de implementação

> **Para quem executa:** SUB-SKILL OBRIGATÓRIA: superpowers:subagent-driven-development (recomendada) ou superpowers:executing-plans, tarefa por tarefa. Os passos usam caixas (`- [ ]`) para acompanhamento.

**Objetivo:** a linha do tempo do mapa começa em 1883 e acende cada empresa no seu prédio na data de inscrição no registro comercial, com velocidade variável até 2018 e o ritmo atual depois.

**Arquitetura:** um único arquivo de lâmpadas (`data/timeline/events.bin.gz`), com a época em 1883-01-01, montado por `scripts/build_timeline.py` a partir de três fontes (eventos do SHAB, datas do registro cantonal, empresas ativas do LINDAS geocodificadas). No `index.html`, um relógio de tela é convertido em dia por uma tabela, e o shader continua recebendo só o dia.

**Stack:** Python 3.11 + Polars (scripts), JavaScript sem build no `index.html` (deck.gl), testes com `python3 -m pytest`.

**Spec:** `tasks/linha-do-tempo-longa.md`. Worktree `~/Projetos/swissviz-cantonal`, branch `feat/cantonal-register`.

## Restrições globais

- No git entra só a data de inscrição (`data/registered.json.gz`: UID + data). Histórico e extratos ficam em `build/`.
- Época: 1883-01-01. Dias em `uint16`; 65535 = sem data.
- Trecho 1883–2018: ~45 s a 1×, no máximo 8 anos por segundo, no mínimo 1 ano a cada 2 s. De 2018 em diante: 365/7 dias por segundo, como hoje.
- Formas jurídicas: o mesmo mapa `LEGAL` (eCH-0097 → letra) para todas as lâmpadas.
- Polars, não pandas. Commits em conventional commits, sem menção a Claude.
- Nenhum dado de pessoa. Nenhuma requisição nova aos registros cantonais neste plano (só LINDAS).
- Verificações no navegador com a skill `terminal-browser`, servidor `bunx serve . -l 3000`.

## Pontos de atenção na revisão

1. **UID com evento no SHAB e também na lista de ativas** → uma lâmpada só (a do SHAB), nunca duas. Teste na Tarefa 3.
2. **Ativa sem coordenada ou sem data** (nem exata nem estimada) → sem lâmpada, e contada no log; o gerador não quebra. Teste na Tarefa 3.
3. **Arquivo de estimativas ausente** (`opening_dates.json.gz` é de outra branch) → o gerador roda sem estimativas e avisa. Teste na Tarefa 3.
4. **Links antigos `?tl=NNN`** (dias desde 2018) → continuam abrindo na mesma data. Verificação na Tarefa 5.
5. **Arrastar a régua para um ano sem nenhuma abertura** (século XIX) → mostra o ano e os contadores certos, sem `NaN` nem salto. Verificação na Tarefa 5.

---

### Tarefa 1: `data/registered.json.gz` a partir do cache

**Arquivos:**
- Modificar: `scripts/fetch_cantonal.py` (função `to_parquet` e nova `registered_dates`)
- Teste: `scripts/test_registered.py`

**Interfaces:**
- Produz: `registered_dates(df: pl.DataFrame) -> pl.DataFrame` com colunas `uid: Int64` (9 dígitos) e `date: Int32` (AAAAMMDD), uma linha por UID, ordenado por `uid`; `write_registered(df, path)` grava `{"asOf", "uid", "date"}` em JSON gzip. `--parquet` passa a chamar as duas e gravar `data/registered.json.gz`.

- [ ] **Passo 1: teste que falha.** Em `scripts/test_registered.py`, um `DataFrame` com as colunas do parquet (`uid`, `host`, `status`, `registered`, `first_journal`, `has_report`) e os casos:

```python
# (uid, host, status, registered, first_journal, has_report)
rows = [
    ("CHE-105.909.036", "zg.chregister.ch", "ok", date(1883, 3, 9), date(1996, 1, 1), None),
    ("CHE-105.909.036", "prestations.vd.ch", "ok", date(1905, 7, 19), None, True),   # dois registros: fica 1883
    ("CHE-111.111.111", "adm.appls.fr.ch", "ok", None, date(1992, 11, 19), False),  # sem data: usa o diário
    ("CHE-222.222.222", "app2.ge.ch", "ok", None, date(2001, 5, 2), True),          # diário com migração: fora
    ("CHE-333.333.333", "zh.chregister.ch", "not_found", None, None, None),         # fora
]
out = registered_dates(df)
assert out.to_dicts() == [{"uid": 105909036, "date": 18830309}, {"uid": 111111111, "date": 19921119}]
```

- [ ] **Passo 2:** `python3 -m pytest scripts/test_registered.py -v` → falha com `registered_dates` indefinida.
- [ ] **Passo 3: implementar** `registered_dates` e `write_registered` em `fetch_cantonal.py`; `to_parquet()` chama as duas no fim e imprime quantos UIDs e a data mínima e máxima. O `import polars` opcional do arquivo continua valendo (as remotas não têm Polars).
- [ ] **Passo 4:** o teste passa. Rodar `python3 scripts/fetch_cantonal.py --parquet` e conferir: Nestlé `105909036 → 18830309`, nenhum `date` nulo, arquivo com alguns MB.
- [ ] **Passo 5: commit** `feat: write the entry date of every company from the cantonal cache` (inclui `data/registered.json.gz`).

### Tarefa 2: empresas ativas com endereço, forma jurídica e coordenada

**Arquivos:**
- Criar: `scripts/active_firms.py`
- Teste: `scripts/test_active_firms.py`

**Interfaces:**
- Consome: `swisstopo_geo.geocode(ev: pl.DataFrame) -> pl.DataFrame` (espera `street`, `houseNumber`, `zip`, `town`, `seat`; devolve `lat`, `lng`, `prec`).
- Produz: `build/lindas_active.parquet` com `uid: Utf8` ("CHE-123.456.789"), `name`, `legalForm` (código eCH de 4 dígitos, ex. "0106"), `street`, `houseNumber`, `zip`, `town`, `seat`, `canton`, `lat`, `lng`, `prec`. Função `parse_rows(csv_bytes: bytes) -> pl.DataFrame` (CSV do SPARQL → colunas acima, sem coordenada) e `main()`.

- [ ] **Passo 1: teste que falha** para `parse_rows`, com um CSV de duas linhas no formato do LINDAS: `uid` "CHE109848706" → "CHE-109.848.706"; `additionalType` "https://ld.admin.ch/ech/97/legalforms/0106" → "0106"; rua e número vindos de `locn:thoroughfare` e `locn:locatorDesignator`; linha sem endereço mantém a empresa com campos vazios (string vazia, não nulo).
- [ ] **Passo 2:** o teste falha.
- [ ] **Passo 3: implementar.** Consulta SPARQL paginada em `https://ld.admin.ch/query` (grafo `https://lindas.admin.ch/foj/zefix`, 50.000 por página, `ORDER BY ?c`, retry como em `lindas_firms`), com `OPTIONAL` para nome (`schema:legalName`), `schema:additionalType`, e do endereço `locn:thoroughfare`, `locn:locatorDesignator`, `schema:postalCode`, `schema:addressLocality`, `schema:addressRegion`; `seat` = nome do município (`schema.ld.admin.ch/municipality` → `schema:name`). User-Agent `cantography-research`. Uma linha por UID. Depois `geocode()` e gravação do parquet. O log imprime o total e a precisão por nível.
- [ ] **Passo 4:** o teste passa. Rodar `python3 scripts/active_firms.py` (≈15 min) e conferir: ~795 mil linhas; `legalForm` nulo em menos de 1%; coordenada em mais de 99%; precisão "building" na mesma ordem da linha do tempo (~90%).
- [ ] **Passo 5: commit** `feat: list the active companies with address, legal form and coordinates`.

### Tarefa 3: `build_timeline.py` com época em 1883 e três fontes

**Arquivos:**
- Modificar: `scripts/build_timeline.py` (vira funções + `main()`)
- Teste: `scripts/test_build_timeline.py`

**Interfaces:**
- Consome: `data/registered.json.gz` (Tarefa 1), `build/lindas_active.parquet` (Tarefa 2), `~/Projetos/swissviz/data/opening_dates.json.gz` (estimativas; opcional), `build/shab_events.jsonl` geocodificado como hoje.
- Produz:
  - `EPOCH = date(1883, 1, 1)`, `NONE = 65535`;
  - `day_of(yyyymmdd: int) -> int` (dias desde `EPOCH`, mínimo 0);
  - `entry_days(registered: dict, estimates: dict | None) -> dict[int, int]` — por UID inteiro, o dia de abertura: data exata; senão estimativa de precisão 1 (o dia) ou 2 (1º de julho do ano); precisão 3 ou ausente → sem entrada;
  - `build_lamps(ev: pl.DataFrame, days: dict[int, int], active: pl.DataFrame) -> tuple[pl.DataFrame, dict]` — lâmpadas (`name`, `t`, `lat`, `lng`, `o`, `c`) ordenadas por (`o`, `c`), e contagens por fonte;
  - `write_timeline(lamps: pl.DataFrame, out_dir: str)` — os dois arquivos, no formato atual.

- [ ] **Passo 1: testes que falham.** Casos, cada um num teste com nome próprio:
  - `test_shab_lamp_keeps_its_days`: empresa com abertura 2019-03-01 e fechamento 2021-06-30 no SHAB → uma lâmpada com `o = day_of(20190301)`, `c = day_of(20210630)`.
  - `test_close_only_gets_entry_date`: fechamento em 2020 sem abertura no SHAB e `days` com 1978-06-30 → `o = day_of(19780630)`; sem entrada em `days` → `o = NONE`.
  - `test_active_without_shab_event_gets_a_lamp`: ativa em `active`, sem evento, com data → lâmpada com `c = NONE`, coordenada e letra da forma jurídica do LINDAS ("0106" → "A").
  - `test_uid_in_shab_and_active_is_one_lamp`: mesma empresa nas duas fontes → uma lâmpada, a do SHAB.
  - `test_active_without_date_or_coordinate_is_dropped`: sem dia, ou `lat` nulo → nenhuma lâmpada; `counts["active_no_date"]` e `counts["active_no_geo"]` somam 1 cada.
  - `test_date_before_epoch_is_day_zero`: `day_of(18500101) == 0`.
  - `test_open_after_close_becomes_close_only`: dia de inscrição posterior ao fechamento → `o = NONE`, `counts["open_after_close"] == 1`.
  - `test_estimates`: `entry_days({}, {1: (20070314, 1), 2: (2007, 2), 3: (0, 3)})` → `{1: day_of(20070314), 2: day_of(20070701)}`; `entry_days({5: 19900101}, None)` funciona sem estimativas.
  - `test_binary_round_trip`: `write_timeline` num diretório temporário; ler de volta `n`, `o` e `c` e comparar.
- [ ] **Passo 2:** `python3 -m pytest scripts/test_build_timeline.py -v` → falham.
- [ ] **Passo 3: implementar.** Mover o código atual para as funções acima sem mudar o tratamento dos eventos do SHAB (pareamento abertura/fechamento, empréstimo de coordenada). `main()` lê as entradas, chama `geocode` nos eventos como hoje, monta e grava; se o arquivo de estimativas não existir, imprime um aviso e segue. O log imprime as contagens por fonte, exatas × estimadas × sem data, e lâmpadas por década de abertura.
- [ ] **Passo 4:** os testes passam.
- [ ] **Passo 5: commit** `feat: build the timeline from 1883 with the register entry dates`.

### Tarefa 4: gerar os dados reais e conferir

**Arquivos:** `data/timeline/events.bin.gz`, `data/timeline/names.json.gz` (regenerados)

- [ ] **Passo 1:** `python3 scripts/build_timeline.py`. Esperado: ~1,1 milhão de lâmpadas; `events.bin.gz` ~6 MB.
- [ ] **Passo 2: conferência** com um script curto de leitura do binário:
  - nenhuma lâmpada com `c < o` (ambos diferentes de 65535);
  - a lâmpada de nome "Nestlé AG" (ou a da Nestlé em Cham/Vevey) tem `o == day_of(18830309)`; a da UBS AG, `o == day_of(19780228)`;
  - lâmpadas com `o` antes de 2018 e `c = NONE` ≈ ativas da P2 com data (centenas de milhares);
  - a distribuição por década cresce sem pico isolado em 1995–1996.
- [ ] **Passo 3: commit** `feat: ship the timeline data from 1883`.

### Tarefa 5: relógio de tela e velocidade variável no `index.html`

**Arquivos:**
- Modificar: `index.html` (bloco "Timeline", linhas ~2364–2760)

**Interfaces:**
- Produz, no objeto `tl`: `clockT: Float32Array` e `clockDay: Float32Array` (nós da tabela tempo de tela → dia, crescentes), `tMax` (segundos a 1× do começo ao fim), e as funções `tlDayAt(t: number) -> number`, `tlTimeAt(day: number) -> number`, `tlRateAt(t: number) -> number` (dias por segundo de tela naquele instante). Estado novo `tlT` (segundos de tela a 1×); `tlDay` passa a ser sempre `tlDayAt(tlT)`.

- [ ] **Passo 1:** `TL_EPOCH = Date.UTC(1883, 0, 1)`; `TL_SPLIT_DAY` = dias de 1883-01-01 a 2018-01-01; `TL_VERSION` + 1; `TL_HIST_SEC = 45`, `TL_MAX_YEARS_PER_SEC = 8`, `TL_MIN_YEARS_PER_SEC = 0.5`.
- [ ] **Passo 2: tabela.** Em `loadTimeline`, depois de `cumOpen`: um nó por ano de `firstDay` a `TL_SPLIT_DAY`. A duração de cada ano é proporcional às aberturas daquele ano, escalada para somar `TL_HIST_SEC`, e então limitada ao intervalo [1/8 s, 2 s] por ano (reescalando os anos não limitados para manter o total perto de 45 s; uma passada basta). De `TL_SPLIT_DAY` a `lastDay`, um único trecho linear a `TL_DAYS_PER_SEC`. `tlDayAt` e `tlTimeAt` fazem busca binária e interpolação linear.
- [ ] **Passo 3: avanço.** `tlTick` soma `dt * tlSpeed` em `tlT` e deriva `tlDay`; fim quando `tlT >= tl.tMax + TL_FADE_SEC`. `setTimeline`, `tlSetPlaying` e o `input` da régua passam a trabalhar com `tlT`.
- [ ] **Passo 4: régua.** `#tl-range` vai de 0 a 1000 e representa `tlT / tl.tMax`; o cursor usa a mesma fração. `tlDateText` mostra só o ano antes de `TL_SPLIT_DAY` e a data completa depois.
- [ ] **Passo 5: URL.** `updateUrl` grava `tl=AAAA-MM-DD`. Na leitura: `AAAA-MM-DD` → dia; número puro (link antigo, dias desde 2018) → `TL_SPLIT_DAY + n`.
- [ ] **Passo 6: verificação no navegador** (`terminal-browser`):
  - `eval`: `tlDayAt(0) === tl.firstDay`; `Math.abs(tlDayAt(tl.tMax) - tl.lastDay) < 1`; `tlTimeAt(tlDayAt(20))` ≈ 20; `tlTimeAt(TL_SPLIT_DAY)` entre 40 e 50; para todo ano do trecho antigo, duração entre 0,125 s e 2 s;
  - tocar do início: o ano avança rápido no século XIX e desacelera; em 2018 não há salto;
  - arrastar a régua até o começo: o rótulo mostra um ano do século XIX, contadores sem `NaN`;
  - abrir `?tl=500`: cai em meados de 2019 (500 dias depois de 01/01/2018).
- [ ] **Passo 7: commit** `feat: play the timeline from 1883 at a variable speed`.

### Tarefa 6: brilho em tempo de tela, gráfico e marcas de ano

**Arquivos:**
- Modificar: `index.html` (`buildTimelineLayer`, `tlDrawHist`, `tlUpdateUi`)

**Interfaces:**
- Consome: `tlRateAt`, `tlTimeAt`, `tl.tMax` (Tarefa 5).

- [ ] **Passo 1:** em `buildTimelineLayer`, `perDay = tlRateAt(tlT) * tlSpeed` no lugar de `TL_DAYS_PER_SEC * tlSpeed`, para que flash e fade durem `TL_FLASH_SEC` e `TL_FADE_SEC` segundos de tela em qualquer época.
- [ ] **Passo 2: gráfico.** `tlDrawHist` desenha no eixo do tempo de tela: antes de `TL_SPLIT_DAY`, uma barra por ano (aberturas do ano, largura = duração do ano na tabela); depois, as barras semanais de hoje. A escala vertical é uma por trecho (o máximo anual e o máximo semanal não se comparam). Marcas de ano em 1900, 1950, 1980, 2000, 2010, 2018 e anuais de 2019 em diante, sem sobrepor rótulos (pular a marca que ficar a menos de 28 px da anterior).
- [ ] **Passo 3: verificação no navegador:** captura da barra em 1400 px e em 390 px de largura; as marcas não se sobrepõem; uma abertura em 1950 e uma em 2020 brilham pelo mesmo tempo (a olho, com a animação pausada e retomada).
- [ ] **Passo 4: commit** `feat: draw the timeline bar on screen time`.

### Tarefa 7: nota na legenda, texto da fonte e celular

**Arquivos:**
- Modificar: `index.html` (dicionários de tradução, linha `tl-source`)

- [ ] **Passo 1: textos.** `tlSource` passa a citar as duas fontes, e uma chave nova `tlBefore2018` aparece abaixo dela, nas quatro línguas:
  - de: "Vor 2018: Unternehmen, die heute bestehen oder seit 2018 gelöscht wurden. Früher gegründete und gelöschte fehlen."
  - en: "Before 2018: companies that exist today or were deleted since 2018. Those opened and closed earlier are missing."
  - fr: "Avant 2018 : entreprises existant aujourd'hui ou radiées depuis 2018. Celles créées et radiées plus tôt manquent."
  - it: "Prima del 2018: imprese esistenti oggi o cancellate dal 2018. Mancano quelle aperte e chiuse prima."
  
  `tlSource` em en: "Entry dates from the cantonal commercial registers; new entries and deletions since 2018 from the Official Gazette of Commerce (SHAB)", com as equivalentes nas outras três.
- [ ] **Passo 2: medição.** No desktop e num celular (ou emulação com limitação de CPU 4×): tempo de carga da linha do tempo, quadros por segundo tocando, memória. Anotar os números em `tasks/linha-do-tempo-longa.md`.
- [ ] **Passo 3: só se o celular ficar abaixo de ~20 quadros por segundo ou falhar ao carregar:** em telas de toque, `loadTimeline` descarta as lâmpadas que não têm nem abertura nem fechamento a partir de `TL_SPLIT_DAY` (as ativas antigas), trata como "já acesa" a abertura anterior a `TL_SPLIT_DAY` das que restam, e a tabela começa em `TL_SPLIT_DAY`. Verificar que a animação de 2018–2026 fica igual à de hoje.
- [ ] **Passo 4: commit** `feat: explain what the timeline shows before 2018`.

### Tarefa 8: regenerar quando a coleta terminar

- [ ] **Passo 1:** com a P1 e a P2 do chregister completas (e de novo quando Friburgo terminar): trazer os caches, `python3 scripts/fetch_cantonal.py --parquet`, `python3 scripts/build_timeline.py`, repetir a conferência da Tarefa 4, subir `TL_VERSION`.
- [ ] **Passo 2:** anotar em `tasks/linha-do-tempo-longa.md` quantas lâmpadas têm data exata, estimada e nenhuma.
- [ ] **Passo 3: commit** `feat: refresh the timeline with the complete register dates`.
