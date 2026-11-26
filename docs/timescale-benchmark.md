# NonoDB time-series vs TimescaleDB — pełny benchmark

Ten dokument zbiera w jednym miejscu środowisko, metodę, definicje zapytań,
wyniki, rozmiar danych, indeksy i zużycie RAM dla porównania NonoDB
(`profile: "timeseries"`) z TimescaleDB na rzeczywistym zbiorze 206 mln wierszy.

Dane surowe i logi poszczególnych etapów: `data/timescale_benchmark_report.md`.

## 1. Środowisko

| Element | NonoDB | TimescaleDB |
|---|---|---|
| CPU | Intel Xeon D-2141I @ 2.20 GHz, 4 vCPU widoczne dla procesu | ten sam host, 4 vCPU widoczne w kontenerze |
| RAM | 14 GiB w środowisku testowym, ~11 GiB dostępne | kontener `pg` z limitem **6 GiB**; CT 103 ma 8 GiB |
| System | Linux, proces in-process | PostgreSQL **17.6**, TimescaleDB **2.21.3**, Alpine |
| Wersja | NonoDB 0.32.0, Rust 1.100.0, build release | oficjalny obraz TimescaleDB |
| Ustawienia | `NONODB_MAP_SIZE=64GiB`, reszta domyślna | `shared_buffers=2GB`, `work_mem=128MB`, `max_connections=25`, `synchronous_commit=off` |
| Kompresja | bucket columnar NTB2/v3, kodeki per kolumna | natywna kompresja TimescaleDB |
| Indeksy | bucket locatory, posting lists, indeks częściowy `symbol_ts_recent` | indeksy B-tree, w tym `symbol_ts_recent` |

Oba procesy widzą 4 vCPU. NonoDB był uruchamiany jako pojedynczy proces
bez klienta sieciowego; TimescaleDB przez `psycopg2` po sieci LAN.

## 2. Zbiór danych

Tabela/kolekcja `ohlc_data`:

| Pole | Typ / znaczenie |
|---|---|
| `time` / `ts` | znacznik czasu, dane do `2026-11-26T08:39:00Z` (`END_MS=1795682340000`) |
| `symbol` | seria, np. `BTC/USD`; setki symboli (429 serii w indeksie) |
| `exchange`, `interval`, `source` | metadane |
| `open`, `high`, `low`, `close`, `volume` | wartości OHLCV |

| Metryka | NonoDB | TimescaleDB |
|---|---:|---:|
| Wiersze | **206 336 608** | **206 345 595** |
| Różnica | — | +8 987 wierszy (**0,004%**); snapshoty różnią się minimalnie |
| Rozmiar bazy | **7 881 351 168 B = 7,34 GiB** (`data.mdb`) | **11 GB** (`pg_database_size('collect_data')`) |
| Indeks `symbol_ts_recent` | **~52,3 MB** | **~435 MB** (`pg_indexes_size` + chunki) |
| Buckety / chunki | 375 522 sealed + 429 open | hypertable, kompresja natywna |

Zapytania po obu stronach filtrują ten sam koniec czasu. Różnica 8 987 wierszy
to 0,004% zbioru i nie zmienia rzędu wielkości wyników.

## 3. Metoda

- NonoDB: `ts-nonodb-tool bench`, jedna rozgrzewka nie jest raportowana,
  następnie **3 przebiegi i mediana**. Czas obejmuje wykonanie zapytania i
  przetworzenie wyniku w procesie, bez sieci.
- TimescaleDB: osobne połączenie `psycopg2`, jedno rozgrzewkowe wykonanie,
  następnie **3 przebiegi i mediana**. Czas obejmuje wykonanie po stronie
  serwera, przesłanie i odebranie wyniku przez LAN.
- Agregacje NonoDB korzystają z materialized rollupów 1h/1d; TimescaleDB w tym
  pomiarze wykonuje zwykłe zapytanie na surowym, skompresowanym hypertable
  (bez continuous aggregate). To ważne zastrzeżenie: TimescaleDB z
  continuous aggregate mogłoby być szybsze w tych agregacjach.
- Wszystkie czasy w milisekundach. „Wygrywa” oznacza krótszy medianowy czas.

## 4. Definicje zapytań

| Etykieta | NonoDB | TimescaleDB |
|---|---|---|
| count ostatniej minuty | `count({symbol:"BTC/USD", ts: [end-60s, end]})` | `count(*) WHERE symbol='BTC/USD' AND time BETWEEN ...` |
| count 1 h | `count({symbol:"BTC/USD", ts: [end-1h, end]})` | analogicznie, okno 1 h |
| count 1 d | `count({symbol:"BTC/USD", ts: [end-1d, end]})` | analogicznie, okno 1 d |
| find 1 d, limit 100 | `find(filter, sort:{ts:-1}, limit:100)` | `SELECT * ... ORDER BY time DESC LIMIT 100` |
| OHLC BTC 30 d | `$match` symbol + 30 d, `$sort ts`, `$group` po `$dateTrunc(ts,"hour")`, `$first open`, `$max high`, `$min low`, `$last close`, `$sum volume`, `$sum 1` | `time_bucket('1 hour', time)`, `first(open,time)`, `max(high)`, `min(low)`, `last(close,time)`, `sum(volume)`, `count(*)` |
| OHLC wszystkie symbole 1 h | filtr 1 h, `$group` po `symbol` + godzina, `$count` grup | `GROUP BY symbol, time_bucket('1 hour', time)` |
| OHLC wszystkie symbole 1 d | filtr 1 d, `$group` po `symbol` + dzień, `$count` grup | `GROUP BY symbol, time_bucket('1 day', time)` |

Uwaga: etykieta „count ostatniej minuty” zastępuje wcześniejsze nieprecyzyjne
„point” w tabelach etapowych. To zapytanie zlicza wiersze w oknie 1 minuty, a
nie pobiera dokumentu po `_id`.

## 5. Wyniki końcowe

Mediany z 3 przebiegów, pełny zbiór 206 mln wierszy:

| Operacja | NonoDB | TimescaleDB | Stosunek NonoDB/Timescale | Wygrywa |
|---|---:|---:|---:|---|
| count BTC/USD, ostatnia 1 minuta | **1,51 ms** | 3,86 ms | 0,39× | NonoDB |
| count BTC/USD, 1 h | **1,13 ms** | 3,79 ms | 0,30× | NonoDB |
| count BTC/USD, 1 d | **3,28 ms** | 5,39 ms | 0,61× | NonoDB |
| find BTC/USD, 1 d, limit 100, sort desc | 7,46 ms | **6,21 ms** | 1,20× | TimescaleDB |
| OHLC 1 h BTC/USD, 30 d | **5,12 ms** | 55,87 ms | 0,09× | NonoDB |
| OHLC 1 h wszystkie symbole, 1 h | 38,67 ms | **2,48 ms** | 15,6× | TimescaleDB |
| OHLC 1 d wszystkie symbole, 1 d | 86,06 ms | **32,89 ms** | 2,6× | TimescaleDB |

Wynik w liczbach:

- NonoDB jest **2,6× szybszy** w count ostatniej minuty, **3,4×** w count 1 h,
  **1,6×** w count 1 d i **10,9×** w OHLC BTC 30 d.
- TimescaleDB jest **1,2× szybszy** w find 1 d z limitem 100, **15,6×** w OHLC
  wszystkich symboli 1 h i **2,6×** w OHLC wszystkich symboli 1 d.
- Cele tego etapu: BTC 30 d **≤150 ms** (jest 5,12 ms), wszystkie symbole 1 d
  **≤300 ms** (jest 86,06 ms) — spełnione.

## 6. Ewolucja w etapach

Te same operacje przed i po kolejnych usprawnieniach (ms, mediany):

| Etap | BTC 30 d | wszystkie 1 h | wszystkie 1 d |
|---|---:|---:|---:|
| 1. bucket locatory | 755,95 | 48,13 | 670,61 |
| 2. szybki count/ordered find | 820,85 | 50,83 | 736,55 |
| 3. zone mapy + Bloom | 821,66 | 59,53 | 755,17 |
| 4. posting lists | 813,95 | 38,66 | 628,75 |
| 5. rollupy 1h/1d | **5,12** | **38,67** | **86,06** |

Pełna tabela punkt/count/find po każdym etapie jest w
`data/timescale_benchmark_report.md`.

## 7. Zużycie RAM

### NonoDB

Mierzone przez `/proc/<pid>/status` i `/proc/<pid>/smaps_rollup` w trakcie
benchmarku:

| Metryka | Wartość |
|---|---:|
| peak `VmRSS` procesu | **27,1 MiB** |
| peak `PSS` procesu | **25,8 MiB** |
| `mmap` LMDB | 64 GiB **wirtualnie**; RSS tylko dotknięte strony |
| page cache OS | poza RSS procesu, zarządzany przez jądro |

### TimescaleDB

Mierzone przez `/sys/fs/cgroup/memory.current` i `memory.stat` kontenera `pg`:

| Metryka | Wartość |
|---|---:|
| `memory.current` | **~2,18–2,23 GiB** |
| `memory.max` | **6 GiB** |
| `shared_buffers` / `shmem` | **~2,07 GiB** (`pg_shmem_allocations` ≈2,19 GB) |
| `anon` | **~38–88 MiB** |
| `file` (z shmem) | **~2,13 GiB** |
| `work_mem` | 128 MB na operację |
| `max_connections` | 25 |

Wniosek: **NonoDB ma ~27 MiB narzutu procesu**, ale do tego dochodzi page
cache OS dla czytanych stron LMDB. **TimescaleDB realnie zajmuje ~2,2 GiB**
głównie z powodu `shared_buffers=2GB`; jego pojedynczy backend ma tylko kilka
MiB pamięci prywatnej, więc nie należy mylić tej liczby z pamięcią całego
serwera. Przy ciężkich sortowaniach/hashach TimescaleDB może rosnąć w stronę
`work_mem × liczba równoległych zapytań`, aż do limitu 6 GiB.

## 8. Indeksy i rozmiar

| Element | NonoDB | TimescaleDB |
|---|---:|---:|
| Baza | 7,34 GiB | 11 GB |
| Indeks `symbol_ts_recent` | ~52,3 MB | ~435 MB |
| Wpisy bucket locator | 751 902 (375 951 bucketów × 2) | — |
| Rollupy | 11 685 wpisów, +2,17 MiB | — |
| Format bucketu | NTB2 / v3 (zone map + Bloom) | natywny chunk |

NonoDB przechowuje metadane serii raz na bucket, a indeksy wtórne jako posting
lists per bucket. TimescaleDB ma większy narzut indeksów, ale zyskuje na
kolumnowym wykonaniu agregacji po wszystkich seriach.

## 9. Poprawność i testy

- `rollups_match_raw_aggregations_and_invalidate_on_write` — rollupy 1h/1d
  porównane z surową agregacją; insert aktualizuje rollup, delete unieważnia.
- `bucket_locator_indexes_follow_insert_delete_and_retention` — locatory po
  insert/seal/delete/retencji.
- `indexed_count_and_ordered_find_match_natural_scan` — count i ordered find
  zgodne ze skanem naturalnym.
- `zone_maps_prune_without_decoding` — pruning zone map/Bloom bez dekodowania.
- Testy unikalności indeksów TS, w tym `(locator, seq)`.
- `cargo test --workspace` — **0 błędów**.
- `cargo clippy --workspace --all-targets -- -D warnings` — czysto.
- `cargo fmt --all -- --check` — czysto.

## 10. Odtworzenie

```bash
# NonoDB — pełny benchmark 206M wierszy
cd /home/docker/nono-fork-rob
NONODB_MAP_SIZE=64GiB END_MS=1795682340000 \
  /tmp/ts-nonodb-tool/target/release/ts-nonodb-tool \
  bench data/timescale_collect_data ohlc_data

# rollupy (jeśli trzeba je odtworzyć po zapisach)
NONODB_MAP_SIZE=64GiB END_MS=1795682340000 \
  /tmp/ts-nonodb-tool/target/release/ts-nonodb-tool \
  rollup data/timescale_collect_data ohlc_data
```

TimescaleDB: te same zapytania SQL z sekcji 4 na bazie `collect_data`
(`TIMESCALE_URL` w `timescaledb.env`).

## 11. Ograniczenia porównania

- NonoDB jest mierzony in-process, TimescaleDB przez sieć i klienta; czasy
  obejmują różne warstwy.
- NonoDB używa rollupów dla OHLC, TimescaleDB w tym pomiarze nie używa
  continuous aggregate.
- Agregacje „wszystkie symbole” pokazują przewagę kolumnowego egzekutora
  TimescaleDB; pojedyncze serie i count wygrywa NonoDB.
- Page cache OS nie jest wliczany do RSS NonoDB, a `shmem` jest wliczany do
  cgroup TimescaleDB — dlatego RAM trzeba porównywać ze świadomością różnicy
  warstw, a nie tylko jedną liczbą.
