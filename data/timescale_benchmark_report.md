# TimescaleDB → NonoDB: import, streamed index backfill i benchmark

Data: 2026-11-26. Baza pozostała na dysku: `data/timescale_collect_data` (nie usuwana).

## Zakres

- Źródło: TimescaleDB 2.21.3 / PostgreSQL 17.6, baza `collect_data`, tabela `public.ohlc_data`.
- Snapshot: `time <= 2026-11-26 08:39:00+00`.
- Wiersze: **206 336 608** (identycznie po obu stronach dla cut-offu).
- Serie: **429** symboli, interwał **1 min**, zakres 2022-01-01 → 2026-11-26.
- NonoDB: kolekcja TS `ohlc_data`, `timeField=ts`, `metaField=symbol`, `granularity=minutes` (bucket 1 dzień), `bucketMaxDocs=4096`, kompresja `max`.
- Czasy Timescale: mediany z 3 przebiegów przez sieć lokalną,
  z górnym ograniczeniem `time <= 2026-11-26 08:39:00+00`.
- Czasy NonoDB: mediany z 3 przebiegów embedded, bez daemona.

## Import

- Czas wstawiania: **6 544 s = 1 h 49 min** → **31 530 rows/s**
- Czas z końcowym `timeseries_stats`: **7 774 s = 2 h 09 min**
- `raw_bytes`: 41 824 819 343 B (~41,8 GB, ~203 B/wiersz)
- `encoded_bytes`: 6 749 400 143 B (~6,75 GB, ~32,7 B/wiersz)
- `data.mdb`: **7 578 755 072 B = 7,58 GB = 7,06 GiB** (~36,7 B/wiersz)
- buckety: **375 522** (wszystkie sealed), serie: **429**
- kompakcja: 17,6 s, odzyskane tylko ~8 MB

## Streamed backfill indeksów

Poprzednia wersja `backfill_timeseries_indexes_txn` robiła
`timeseries::all_docs()` i ładowała wszystkie 206 mln dokumentów do RAM.
Teraz:

- `timeseries::scan_index_docs` najpierw zbiera tylko klucze bucketów,
- dekoduje jeden bucket na raz (maks. `bucketMaxDocs` wierszy),
- przekazuje ograniczone batche do writera indeksu w tej samej transakcji,
- nie tworzy wektora wszystkich dokumentów.

Test: `index_backfill_streams_across_many_buckets`.
Dodatkowo planner rozpoznaje zakresowe predykaty indeksów częściowych
(np. query `ts >= 150` może użyć indeksu z `ts >= 100`).

## Indeks częściowy

Zbudowany indeks `symbol_ts_recent` na `symbol + ts` z predykatem
`ts >= 2026-10-27`, czyli ostatnie ~30 dni.

- czas budowy: **1 621 s = 27 min** (skan całej kolekcji, zapis ~637 tys. wpisów)
- wzrost `data.mdb`: **+143 527 936 B = +137 MiB**
- `data.mdb` po indeksie: **7 722 283 008 B = 7,72 GB = 7,19 GiB**
- `explain` dla zapytania 30-dniowego: `"index_used": true`,
  `"access": "index_scan"`, `"index": "symbol_ts_recent"`

Pełny indeks na wszystkie 206 mln wierszy nadal nie jest praktyczny:
z próbki i z częściowego indeksu wychodzi ~225 B/wpisy, czyli **~45–55 GB**
na całą kolekcję. Streamed backfill usuwa problem RAM od `all_docs`, ale
cały indeks nadal byłby budowany w jednej transakcji i zjadałby większość
wolnego miejsca.

## Etap 1: bucket locator indexes

Dodano dwa kompaktowe indeksy w sub-bazie `tsb:<collection>`:

- `0x10 | series_hash(8) | window(8) | salt(4) | serial(2)` — dostęp po serii i czasie,
- `0x11 | window(8) | series_hash(8) | salt(4) | serial(2)` — dostęp po czasie dla wszystkich serii.

Klucz ma 23 B. Indeks jest utrzymywany przy `insert`, `insert_many`,
`seal_bucket`/`seal_all`, `persist_bucket`, `delete`, `delete_by_id`,
aktualizacji przenoszącej wiersz oraz retencji. Dla starych kolekcji
`Store::open` uruchamia jednorazowy backfill po kluczach bucketów (bez
dekodowania wierszy) i zapisuje marker `0x0F`. Planner (`lookup_indexed_buckets`)
używa indeksu, gdy filtr zawiera równość `metaField` i/lub zakres czasu.

- wpisy: **751 902** (375 951 bucketów × 2 kierunki; 375 522 sealed + 429 open)
- rozmiar kluczy: **17 293 746 B ≈ 16,5 MiB**
- wzrost `data.mdb`: **+44 838 912 B ≈ +42,8 MiB** względem wersji z indeksem częściowym
- `data.mdb` po etapie: **7 767 121 920 B = 7,23 GiB**
- backfill: tylko iteracja kluczy, bez `all_docs()`; czas otwarcia po zbudowaniu ~0,14 s

Benchmark po etapie (koniec danych `2026-11-26 08:39:00`, mediany 3 przebiegów):

| Operacja | TimescaleDB | NonoDB bez indeksu | NonoDB partial idx | NonoDB bucket locator |
|---|---:|---:|---:|---:|
| punkt: BTC/USD, 1 minuta | 3,98 ms | 32,11 ms | 25,17 ms | **2,61 ms** |
| count BTC/USD, 1 h | 3,91 ms | 26,68 ms | 25,43 ms | **2,28 ms** |
| count BTC/USD, 1 d | 5,60 ms | 39,92 ms | 38,75 ms | **14,11 ms** |
| find BTC/USD, 1 d, limit 100, sort desc | 6,29 ms | 57,71 ms | 20,32 ms | **17,78 ms** |
| OHLC 1 h, BTC/USD, 30 d | 56,85 ms | 780,18 ms | 808,51 ms | 755,95 ms |
| OHLC 1 h, wszystkie symbole, 1 h | 2,65 ms | 183,02 ms | 153,28 ms | **48,13 ms** |
| OHLC 1 d, wszystkie symbole, 1 d | 33,37 ms | 854,75 ms | 808,54 ms | 670,61 ms |

Wnioski po etapie 1:

- point i count 1 h są już na poziomie Timescale,
- count 1 d i find 1 d poprawiły się 2–3×, ale nadal dekodują cały dzienny
  bucket i sortują wynik — temat etapu 2,
- duże agregacje zyskują głównie na iteracji kluczy (1 h wszystkie symbole
  3,8×); sam koszt dekodowania dziennych bucketów pozostaje.

## Etap 2: szybkie count i ordered find

- `count` dla filtrów `metaField` (równość) + zakres czasu nie dekoduje już
  dokumentów: najpierw sprawdza nagłówek bucketu, a przy częściowym zakresie
  dekoduje tylko kolumnę czasu i bitmapę usunięć. Pełne pokrycie bucketu to
  `n_rows - deleted`.
- `find` z sortowaniem po czasie i `limit` idzie po locatorach w kolejności
  indeksu (rosnąco/malejąco), grupuje ten sam bucket i pobiera tylko `skip+limit`
  najnowszych/najstarszych wierszy. Dekodowanie kolumn jest blokowe: bloki bez
  wybranych wierszy są pomijane.
- Dla zapytań `meta + czas + sort po czasie` bucket locator jest preferowany
  nawet gdy planner ma kandydatów z indeksu wtórnego; filtr resztkowy nadal jest
  egzekwowany, a ścieżka geo (`$near`) pozostaje przy indeksie geo.
- `explain` dla TS pokazuje `access=index_scan`, `index=bucket_locator` oraz
  `bucket_index_used=true`, gdy użyto bucket locatora.
- Test `indexed_count_and_ordered_find_match_natural_scan` porównuje count/find
  po indeksie z pełnym skanem po usunięciu markera indeksu.

Benchmark po etapie 2 (`2026-11-26 08:39:00`, mediany 3 przebiegów):

| Operacja | TimescaleDB | Etap 1 | Etap 2 | Cel |
|---|---:|---:|---:|---:|
| punkt: BTC/USD, 1 minuta | 3,98 ms | 2,61 ms | **1,78 ms** | ≤ 5 ms |
| count BTC/USD, 1 h | 3,91 ms | 2,28 ms | **1,62 ms** | ≤ 5 ms |
| count BTC/USD, 1 d | 5,60 ms | 14,11 ms | **3,57 ms** | ≤ 5 ms |
| find BTC/USD, 1 d, limit 100, sort desc | 6,29 ms | 17,78 ms | **6,58 ms** | ≤ 10 ms |
| OHLC 1 h, BTC/USD, 30 d | 56,85 ms | 755,95 ms | 820,85 ms | — |
| OHLC 1 h, wszystkie symbole, 1 h | 2,65 ms | 48,13 ms | 50,83 ms | — |
| OHLC 1 d, wszystkie symbole, 1 d | 33,37 ms | 670,61 ms | 736,55 ms | — |

Wszystkie cztery cele etapu 2 są spełnione. Duże agregacje nadal są
zdominowane przez dekodowanie pełnych dokumentów i sortowanie/materializację —
to zakres etapów 3–5 (zone map/Bloom, posting lists, rollupy).

## Etap 3: zone mapy i Bloom per bucket

Format bucketu podniesiony do wersji 3. Po nagłówku (meta) zapisywany jest
blok zone-map:

- min/max `f64` dla kolumn numerycznych i czasu,
- 256-bajtowy Bloom (3 hashe) po wartościach tekstowych/boolean z kluczem
  `field\0value`.

Stare buckety w wersji 1/2 są nadal czytane (wersja 3 jest opcjonalna).
Filtry zakresowe/liczbowe oraz równości tekstowe są sprawdzane przed
dekodowaniem kolumn. `explain` raportuje `buckets_total` i `buckets_pruned`.

Migracja istniejącej bazy: `ts-nonodb-tool reencode` przepisał
**375 522 / 375 522** bucketów do wersji 3.

- rozmiar `data.mdb` po etapie: **7 970 258 944 B = 7,42 GiB**
- wzrost względem etapu 2: **+203 137 024 B ≈ +193,7 MiB** (~542 B/bucket)
- wpisy bucket index bez zmian: 751 902

Pruning na pełnej bazie (`BTC/USD`, 1775 kandydatów = 887 dni × 2 kierunki):

| Filtr | Kandydaci | Pominięte | Czas |
|---|---:|---:|---:|
| `source = "__missing__"` (Bloom) | 1775 | **1774** | 4,75 ms |
| `close > 1e12` (zone min/max) | 1775 | **1774** | 4,83 ms |

Jeden kandydat to pusty open-bucket pointer bez payloadu. Bez zone map filtr
`source="__missing__"` musiałby zdekodować wszystkie 887 dziennych bucketów.

Benchmark po etapie 3 (`2026-11-26 08:39:00`, mediany 3 przebiegów):

| Operacja | TimescaleDB | Etap 2 | Etap 3 |
|---|---:|---:|---:|
| punkt: BTC/USD, 1 minuta | 3,98 ms | 1,78 ms | **1,25 ms** |
| count BTC/USD, 1 h | 3,91 ms | 1,62 ms | **1,10 ms** |
| count BTC/USD, 1 d | 5,60 ms | 3,57 ms | **3,30 ms** |
| find BTC/USD, 1 d, limit 100, sort desc | 6,29 ms | 6,58 ms | **7,16 ms** |
| OHLC 1 h, BTC/USD, 30 d | 56,85 ms | 820,85 ms | 821,66 ms |
| OHLC 1 h, wszystkie symbole, 1 h | 2,65 ms | 50,83 ms | 59,53 ms |
| OHLC 1 d, wszystkie symbole, 1 d | 33,37 ms | 736,55 ms | 755,17 ms |

Zone mapy nie zmieniają ścieżki zwykłych zapytań `meta + czas` (są tam
pomijane bez parsowania), a całkowicie odrzucają buckety dla filtrów na
dodatkowych kolumnach. Duże agregacje pozostają bez zmian — etapy 4–5.

## Etap 4: posting lists i kompaktowe locatory

Indeksy wtórne na TS nie zapisują już jednego wpisu na wiersz z 40-znakowym
`_id`. Zamiast tego:

- klucz wpisu: `0x00 0x00 | encoded_value | hash(8) | salt(4) | window(8) | serial(2)`,
- wartość: bitmapa numerów `seq` w buckecie,
- locator ma 22 B zamiast 40–52-znakowego `_id`, a bitmapa rośnie tylko do
  najwyższego `seq` w danym buckecie.

`IndexDef.postings=true` oznacza nowy format. `createIndex`, `createIndexes`,
`dropIndex` i `reindex` obsługują go; unikalność sprawdza bity `(locator, seq)`,
więc dwa różne wiersze w tym samym buckecie nie są już mylone. Geo indeksy
pozostają przy starym formacie (współrzędne w kluczu), a indeksy wtórne bez
postings nadal są czytane.

Przebudowa istniejącego `symbol_ts_recent` (predykat `ts >= 2026-10-27`):

- czas budowy: **1 650,85 s = 27,5 min**
- `data.mdb` po kompakcji: **7 879 077 888 B = 7,34 GiB**
- bez indeksu po etapie 3: ~7 826 731 008 B, czyli nowy indeks to
  **~52,3 MB ≈ 49,9 MiB**
- poprzedni per-wiersz: +143 527 936 B (137 MiB) → **~63% mniej**
- Timescale `pg_indexes_size`/chunki: **435 MB** → indeks NonoDB ~8× mniejszy
- `explain` dla 30-dniowego BTC/USD: `index=symbol_ts_recent`,
  `access=index_scan`, `matched=35 508` (zgodne z surowymi danymi)

Benchmark po etapie 4 (`2026-11-26 08:39:00`, mediany 3 przebiegów):

| Operacja | TimescaleDB | Etap 3 | Etap 4 |
|---|---:|---:|---:|
| punkt: BTC/USD, 1 minuta | 3,98 ms | 1,25 ms | **1,46 ms** |
| count BTC/USD, 1 h | 3,91 ms | 1,10 ms | **1,06 ms** |
| count BTC/USD, 1 d | 5,60 ms | 3,30 ms | **3,90 ms** |
| find BTC/USD, 1 d, limit 100, sort desc | 6,29 ms | 7,16 ms | **7,86 ms** |
| OHLC 1 h, BTC/USD, 30 d | 56,85 ms | 821,66 ms | 813,95 ms |
| OHLC 1 h, wszystkie symbole, 1 h | 2,65 ms | 59,53 ms | **38,66 ms** |
| OHLC 1 d, wszystkie symbole, 1 d | 33,37 ms | 755,17 ms | **628,75 ms** |
| filtr brakującego stringa (Bloom) | — | 4,75 ms | 6,26 ms |
| filtr close > 1e12 (zone map) | — | 4,83 ms | 11,12 ms |

Czasy zwykłych zapytań pozostają na poziomie etapu 3; główny zysk etapu 4 to
rozmiar indeksu i brak rozdęcia kluczy o `_id`. Agregacje wszystkich symboli
są nieco szybsze po kompakcji bazy.

## Etap 5: materialized rollupy 1h/1d

Dodano sub-bazę `tsr:<collection>` z dwoma poziomami rollupów:

- `0x01 | series_hash | window_start` → 64-bajtowy wiersz OHLCV,
- `0x02 | series_hash | window_start` → ten sam wiersz dla dni,
- wiersz: `min_ts, max_ts, open, high, low, close, volume, n`.

Rollup jest budowany przez `build_rollups` (zakres + marker pokrycia),
utrzymywany przy `insert`/`insert_many` (gdy pokrycie istnieje) i unieważniany
przy `delete`/`update`/`retention`.
Zapytanie agregujące używa rollupów, gdy filtr dotyczy tylko `metaField` i
czasu; częściowe okna na granicach zakresu są doczytywane z surowych danych
(dla dni najpierw z rollupów godzinowych), a `$sum:1` jest tłumaczone na
`$sum:"$n"`. Dzięki temu `first/last/open/high/low/close/volume` zachowują
znaczenie również dla niepełnych zakresów.

Budowa rollupów dla 30 dni (wszystkie serie): **4,99 s**, 11 685 wpisów.
Wzrost `data.mdb`: **+2 273 280 B ≈ +2,17 MiB** (7 881 351 168 B).

Benchmark po etapie 5 (`2026-11-26 08:39:00`, mediany 3 przebiegów):

| Operacja | TimescaleDB | Etap 4 | Etap 5 | Cel |
|---|---:|---:|---:|---:|
| OHLC 1 h, BTC/USD, 30 d | 56,85 ms | 813,95 ms | **5,12 ms** | ≤ 150 ms |
| OHLC 1 h, wszystkie symbole, 1 h | 2,65 ms | 38,66 ms | **38,67 ms** | — |
| OHLC 1 d, wszystkie symbole, 1 d | 33,37 ms | 628,75 ms | **86,06 ms** | ≤ 300 ms |
| punkt: BTC/USD, 1 minuta | 3,98 ms | 1,46 ms | 1,51 ms | — |
| count BTC/USD, 1 h | 3,91 ms | 1,06 ms | 1,13 ms | — |
| count BTC/USD, 1 d | 5,60 ms | 3,90 ms | 3,28 ms | — |
| find BTC/USD, 1 d, limit 100, sort desc | 6,29 ms | 7,86 ms | 7,46 ms | — |

Oba cele etapu 5 są spełnione z dużym zapasem. Test
`rollups_match_raw_aggregations_and_invalidate_on_write` porównuje rollupy
godzinowe i dzienne z surową agregacją oraz sprawdza unieważnienie po zapisie.

### Podsumowanie wszystkich etapów

| Operacja | TimescaleDB | Etap 1 | Etap 2 | Etap 3 | Etap 4 | Etap 5 |
|---|---:|---:|---:|---:|---:|---:|
| punkt: BTC/USD, 1 minuta | 3,98 ms | 2,61 | 1,78 | 1,25 | 1,46 | **1,51** |
| count BTC/USD, 1 h | 3,91 ms | 2,28 | 1,62 | 1,10 | 1,06 | **1,13** |
| count BTC/USD, 1 d | 5,60 ms | 14,11 | 3,57 | 3,30 | 3,90 | **3,28** |
| find BTC/USD, 1 d, limit 100, sort desc | 6,29 ms | 17,78 | 6,58 | 7,16 | 7,86 | **7,46** |
| OHLC 1 h, BTC/USD, 30 d | 56,85 ms | 755,95 | 820,85 | 821,66 | 813,95 | **5,12** |
| OHLC 1 h, wszystkie symbole, 1 h | 2,65 ms | 48,13 | 50,83 | 59,53 | 38,66 | **38,67** |
| OHLC 1 d, wszystkie symbole, 1 d | 33,37 ms | 670,61 | 736,55 | 755,17 | 628,75 | **86,06** |

Rozmiar `data.mdb` na końcu: **7 881 351 168 B = 7,34 GiB** (plus 2,17 MiB
rollupów względem etapu 4). Baza importowa `data/timescale_collect_data`
została zachowana.

## Benchmark zapytań

| Operacja | TimescaleDB | NonoDB bez indeksu | NonoDB z indeksem częściowym |
|---|---:|---:|---:|
| punkt: BTC/USD, 1 minuta | 3,98 ms | 32,11 ms | 25,17 ms |
| count BTC/USD, 1 h | 3,91 ms | 26,68 ms | 25,43 ms |
| count BTC/USD, 1 d | 5,60 ms | 39,92 ms | 38,75 ms |
| find BTC/USD, 1 d, limit 100, sort desc | 6,29 ms | 57,71 ms | **20,32 ms** |
| OHLC 1 h, BTC/USD, 30 d | 56,85 ms | 780,18 ms | 808,51 ms |
| OHLC 1 h, wszystkie symbole, 1 h | 2,65 ms | 183,02 ms | 153,28 ms |
| OHLC 1 d, wszystkie symbole, 1 d | 33,37 ms | 854,75 ms | 808,54 ms |

Indeks częściowy najbardziej pomógł `find ... limit 100` (2,8×).
`count` i duże agregacje nie poprawiły się istotnie, bo nadal dekodują
kandydatów i mają koszt `_id`/locatorów; to kolejny temat do optymalizacji.

## Poprawność po indeksie

- Liczba wierszy dla cut-offu: **206 336 608 = 206 336 608**.
- Agregacja dzienna wszystkich symboli za 30 dni:
  - Timescale: groups=761, rows=637 455, volume=54 641 813,91147192
  - NonoDB: groups=761, rows=637 455, volume=54 641 813,91147196
- Agregacja 1 h BTC/USD za 30 dni:
  - Timescale: groups=598, rows=35 508, volume=4 215,540347999997
  - NonoDB: identycznie
- Próbka ostatnich świec BTC/USD: open/high/low/close/volume/source zgodne.
- `exchange` w źródle jest NULL dla całej tabeli; w NonoDB również NULL.

## Wnioski

1. Import i format TS działają poprawnie — agregacje zgadzają się co do wiersza.
2. Rozmiar: NonoDB 7,58 GB bez indeksu / 7,72 GB z indeksem częściowym
   vs Timescale 7,94 GB (hypertabela).
3. Streamed backfill działa: indeks częściowy buduje się bez `all_docs()`.
4. Indeks częściowy daje wyraźny zysk na `find` z limitem, ale nie na `count`
   i dużych agregacjach.
5. Pełny indeks TS wymaga jeszcze zmiany layoutu (mniejszy locator) i/lub
   budowy porcjami; sama streamowana iteracja nie rozwiązuje rozmiaru.

## Pliki

- Dane: `data/timescale_collect_data/`
- Narzędzie: `data/timescale-nonodb-tool/`
- Logi: `/tmp/timescale_import.log`, `/tmp/nonodb_bench_final.log`,
  `/tmp/nonodb_bench_partial_index2.log`, `/tmp/timescale_bench_final.jsonl`
