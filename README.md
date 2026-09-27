# NYC TLC Trip Metrics — full year 2023

**A monthly "are trips running on time?" number for NYC yellow taxis, built from public open data,
with a gate that refuses to publish a number it can't stand behind.**

The data comes from the NYC Open Data API (Socrata dataset `4b4i-vxc`, "2023 Yellow Taxi Trip
Data") — **12 months, 38,310,122 trips**. The pipeline re-runs in **23 seconds**. The whole repo is
**4.8 MB**; the trip data is not in git, it comes back with one command.

📊 **[Evidence dashboard](output/evidence_table.md)** — the results table, regenerated from the
pipeline's own output, so it can never disagree with the numbers.

---

## At a glance

| | |
|---|---|
| **The KPI** | **Trip Time Reliability** — the share of trips that finished within the expected time for their distance and hour of day |
| **The year** | 80.02% in January → **63.17%** in December |
| **Data trust** | Every month reconciled against the API's own row count — **12 of 12** ✓ |
| **Publish decision** | **PUBLISH** · 97.97% of rows valid (37,532,268) · 777,854 rejected *but kept, with reasons* |
| **Safety net** | 16 validation rules with named owners · 42 automated tests |
| **Cost** | 13 min one-off download · 23 s to rebuild every number |

**How to read the rest:** each section makes a plain claim first, then shows the evidence, then
names the limits. Nothing is asserted without a number you can check.

---

## 1. The problem, and the one number we publish

Leadership wants a dependable monthly view of how NYC yellow taxis are performing. The data is
messy in ways that are easy to miss: it's reported by technology providers rather than by TLC
itself, TLC explicitly disclaims its accuracy, and 3.4% of rows are missing pieces of information.

So the deliverable isn't just a number. It's a number **plus the evidence that it's safe to publish**
— and a gate that says *don't* when it isn't.

### The KPI: Trip Time Reliability (M2)

> **The share of valid trips that finished within the expected time for their distance band and
> time-of-day.**

**Why not just "average trip time"?** Because a 30-minute trip is *slow* if it's 1 mile at 3am, and
*fast* if it's 15 miles at 5pm. Judging a trip by the clock alone is like judging a person by
height alone — you need context. So each trip is judged against **its own bar**:

- Sort all trips that looked like it — same distance group, same time of day — and take the time
  that **80% of them** finished within. That's the bar. (We call it the P80 of the baseline month.)
- A trip is "on time" if it beat its own bar. The KPI is the share of trips that did.

The bar comes from **January 2023** and stays fixed all year, so every month is judged against the
same ruler.

**The fairness check:** January is scored by its own ruler, so it must land near 80%. It lands at
**80.02%**. If it hadn't, the ruler would be unfair and the whole KPI would be suspect.

The five metrics around it: **M1** valid-trip rate (can we publish at all?) · **M2** reliability
(the KPI) · **M3** duration P50/P90 (how bad is the tail?) · **M4** invalid rate (data-quality cost,
always disclosed) · **M5** slow-trip share under 6 mph (the driver to chase).

## 2. What we found

| Month | Valid (M1) | **Reliability (M2)** | P50 / P90 | Invalid (M4) | Slow <6 mph (M5) |
|---|---:|---:|---:|---:|---:|
| Jan *(the baseline)* | 98.50% | **80.02%** | 11.6 / 27.9 min | 1.50% | 8.77% |
| Feb | 98.57% | 77.43% | 11.9 / 28.5 | 1.43% | 9.78% |
| Mar | 98.56% | 74.11% | 12.2 / 30.7 | 1.44% | 11.94% |
| Apr | 98.77% | 73.37% | 12.4 / 31.8 | 1.23% | 12.13% |
| May | 98.59% | 68.64% | 13.1 / 34.0 | 1.34% | 14.78% |
| Jun | 98.55% | 69.94% | 12.9 / 33.5 | 1.45% | 14.01% |
| Jul | 98.28% | 73.38% | 12.5 / 32.1 | 1.72% | 12.74% |
| Aug | 97.96% | 74.43% | 12.5 / 32.2 | 2.04% | 11.47% |
| Sep | 96.53% | 63.29% | 13.7 / 36.5 | 3.47% | 19.55% |
| Oct | 96.54% | 63.98% | 13.6 / 34.9 | 3.46% | 18.48% |
| Nov | 96.90% | 63.76% | 13.4 / 34.2 | 3.10% | 19.96% |
| Dec | 97.82% | **63.17%** | 13.4 / 34.4 | 2.18% | 20.76% |

**Where operations should look (December):** the worst pickup hours are **16:00–20:00** (reliability
54.5%). The worst zones are **Penn Station / Madison Sq (44.4%)**, Garment District (47.8%) and East
Chelsea (50.0%) — Manhattan core. The best are Upper West Side and Manhattan Valley, around 85%.
**Airport trips are only 5% of volume but fall hardest: 80.0% → 58.5%** (median 35 → 42 minutes).

**Is the decline real, or did the trips just get longer?** Fair question — if December simply had
more long airport trips, the fall could be a mix effect rather than slower trips. We tested it by
reweighting every month to January's distance mix: **the number moves by at most 0.15 points.** So
the slowdown is real *within every distance group*. (Durations did rise — median 11.6 → 13.4 min.)

**What we won't claim:** *why* it happened. There is no traffic, weather or road-closure data here,
so every statement in this project is an association ("slow trips cluster at 4pm in Midtown"),
never a cause ("congestion caused it").

## 3. Where the data comes from

| Source | Who owns it | What we take | How |
|---|---|---|---|
| **`4b4i-vxc` — 2023 Yellow Taxi Trip Data** | NYC TLC, submitted by technology providers | every trip fact: times, distance, zone, money fields, rate code | **API**, month by month |
| TLC trip-record page + user guide | NYC TLC | what each field means, which values are legal, their accuracy disclaimer | web page |
| `taxi_zone_lookup.csv` | NYC TLC | zone names and boroughs (265 rows) | file download |
| Socrata `8meu-9t5y` — taxi zones | NYC Open Data / TLC | zone shapes for the map + a cross-check of the zone names | **API** |

**Three business questions, three answers:** how reliable is trip time (Q1) → the trip records ·
where and when are trips slow (Q2) → trip records + the zone reference, because the 2023 data
carries TLC's own zone IDs · can we prove we downloaded everything (Q3) → the API's own row count.

**What this source does *not* have** (all recorded, none silently patched): **no coordinates** —
location arrives as a zone ID, never a street address · no driver, vehicle, traffic or weather data
· no trip ID · 3.4% of rows missing trip metadata · `ratecode = 99` and `vendor = 6` are values the
TLC guide never defines.

## 4. How we know we got everything

**The rule: a "200 OK" proves nothing. Equal counts prove completeness.**

For each of the 12 months we asked the API *"how many rows exist between these dates?"*, downloaded
every page, and compared:

| Month | API says | We stored | Match | Pages |
|---|---:|---:|:--:|---:|
| 2023-01 … 2023-12 | 38,310,122 | 38,310,122 | **12/12 ✓** | 766 |

Five things make that trustworthy rather than hopeful:

- **Raw responses are kept.** Every page the API sent is stored as a file (gzipped, byte-for-byte).
  The parquet we analyse is a re-typing of those pages — never an edit.
- **A fingerprint per month.** `data/manifest.json` holds a SHA-256 of each month's file, so if a
  single byte ever changed, we'd know.
- **Nothing is quietly dropped.** We only skip re-downloading a month that already exists — and even
  then we re-verify it against the API. An interrupted 13-minute download resumes where it stopped.
- **Bad connections are handled.** Timeouts and 429/5xx errors are retried 5 times with backoff.
- **A failed count is recorded as UNKNOWN, never as zero** and never as a pass. If we can't verify a
  month, we say so instead of assuming it's fine.

One more check worth knowing about: because we page through results by offset, an unstable server
sort could silently repeat or skip rows. So we fingerprint each row by its *content*
(times + zones + distance + fare) and count duplicates: **2 in 38.3M rows** — nothing systematic.

## 5. Can we trust the rows?

**Mindset: state the business assumption, turn it into a checkable rule, give the rule an owner,
then publish the failures instead of hiding them.** Sixteen rules live in
`src/nyc_pipeline/rules.py`, and every threshold is in `config.yaml` with a comment showing the rate
we actually measured.

**Two words used throughout:**
- **Quarantine** — the trip leaves the published numbers (it can't give a trustworthy duration,
  distance or location). The row is **kept on disk with a reason code**, never deleted.
- **Flag** — the trip stays in the numbers, but the oddity is counted, reported and needs an answer
  from whoever owns that field. A flag is a question, not a deletion.

### The rules and what they found

| Rule | Expectation | Owner | Found | Status |
|---|---|---|---:|---|
| R00 timestamps readable | both times parse | FDE | 0.000% | PASS |
| R01 dropoff after pickup | positive duration | FDE | 0.041% | PASS |
| R02 duration ≤ 24 h | no meter-error trips | **client** | 0.001% | PASS |
| R03 pickup in its own month | no mis-filed rows | FDE | 0.000% | PASS |
| R04/R05 zone IDs valid | pickup & dropoff zone 1–265 | FDE | 0.000% | PASS |
| R06 distance in (0, 200] mi | the KPI needs a distance | FDE | 2.022% | **WARN** |
| R07 passenger count 1–6 | plausible | client | 4.941% | PASS |
| R08 money not negative | no negative fares | finance | 1.001% | PASS |
| R09 total = sum of 8 parts (±$5) | money adds up | finance | 0.000% | PASS |
| R10 rate code is 1–6 | known rate code | client | 3.975% | PASS |
| R11 store-and-forward Y/N | known flag | client | 3.418% | PASS |
| R12 vendor is 1, 2 or 6 | known vendor | FDE | 0.000% | PASS |
| R13 payment type 1–6 | known payment | client | 3.418% | PASS |
| R14/R15 zone is a real place | not "Unknown"/"NA" | FDE | 1.011% / 1.441% | PASS |
| U1–U5, L1 | undefined values & published limits | client / TLC | — | UNKNOWN |

**Result: 15 PASS · 1 WARN · 0 FAIL · 6 UNKNOWN → gate PUBLISH.**
Valid coverage 97.97% (37,532,268 of 38,310,122). The one WARN is real: **774,434 trips
(2.02%) recorded zero distance**, rising to 3.4% by September–November. We don't know why (U3), so
it's disclosed rather than explained away.

### Two things profiling changed

**1. A 3.4% cohort with missing trip metadata — 1,309,356 rows.** No rate code, no passenger count,
no store-and-forward flag, `payment_type = 0`, and blank in the two newest columns. All from one
vendor.

The tempting read is "corrupt data, drop it". But the trips look real: median 16.6 minutes, average
18.5 miles, average fare $22 — valid times, valid distances. What's missing is *meter paperwork*,
and the KPI only uses time and distance. So we **kept them, flagged them (R10/R11/R13), wrote the
assumption down (A6/A7), and asked the client to explain the vendor behaviour (U5).**

For scale: dropping them would have cut published coverage from 97.97% to 95.32% — a million real
trips removed for a reason unrelated to trip validity. And the KPI would have barely moved
(63.12% instead of 63.21% in December), which is exactly why this kind of decision needs a
principle rather than a hunch: **when the wrong choice is nearly invisible in the output, nothing
but a stated principle protects you.**

**2. The money check had to be rebuilt.** 2023 added two new surcharge fields, and inclusion varies
by row: 26.7% of rows differ from their own component sum by $1–2.50 — but **none differ by more than
$5**. So the rule uses a $5 tolerance, chosen from the measurement rather than a guess, and it stays
a *flag* because money isn't in the KPI (and TLC's money fields are not trustworthy enough to
publish — see L5).

### A bug this surfaced

Chasing that cohort exposed a flaw in my validation code: a rule evaluated over a **missing**
value was being counted as a **pass**. `CASE WHEN NOT ok` returns NULL when `ok` is NULL, and NULL
fell through to "no failure". With 1.3M missing rate codes I was under-reporting my own data
quality. It now reads `COALESCE(ok, false)` — **a rule that cannot be evaluated counts as failed** —
and there's a test that locks the behaviour in.

## 6. How the trips are modelled

### The grain: one row = one valid trip

The catch: **the data has no trip ID**, so we can't prove two rows are different trips. Two guards
replace that missing key:

**Guard 1 — nothing appears or disappears.** For every month:

```
rows read from the file .............. 3,066,726   (January 2023)
  ├─ valid trips (in the KPI) ........ 3,020,708
  └─ quarantined trips (rejected) ....   46,018
                                          ─────────
3,020,708 + 46,018 = 3,066,726 ✓
```

The pipeline **asserts that equality every month** and aborts the job if it ever breaks. Think of it
as a warehouse inventory check: everything that came in is either on the shelf or in the reject bin
— and the reject bin is *counted*, not swept away.

**Guard 2 — nothing gets counted twice.** We fingerprint each row by its content
(pickup time, dropoff time, both zones, distance, fare, total) and compare rows against distinct
fingerprints. No single field is unique, but the combination is: **2 duplicates in 38.3M rows**.

### The tables in the model

| Table | One row is… | Rows |
|---|---|---:|
| raw month | one trip as retrieved | ~3.2M per month |
| raw page | one API response page (kept as proof) | 766 total |
| **fact_trip** | one **valid** trip — the table everything is computed from | 37.5M |
| quarantine_trip | one **rejected** trip + why | 777,854 |
| expected_duration | one (distance group × time-of-day) cell — the ruler | 30 |
| zone | one taxi zone | 263 |

### What we observed vs what we calculated

| Fact | Observed or calculated? | Source / note |
|---|---|---|
| Pickup & dropoff time, **zone ID** | **observed** | the data's own fields · 1% carry a placeholder zone |
| Trip distance | **observed** | 2% are zero (R06 / U3) |
| Rate code, payment type, passenger count | observed but incomplete | 3.4% missing (U5) |
| Money fields | observed but inconsistent | 26.7% off by $1–2.50 (L5) |
| Duration, speed, distance group, time-of-day group | **calculated once**, in the pipeline | so every number uses the same definition |
| **Coordinates (lat/lon)** | **not in this source at all** | no street-level location, no map-by-address |
| Traffic, weather, driver, interventions | **not in this source at all** | so: associations only, never causes |

### Four modelling decisions worth knowing

1. **Money stays a flag, not a feature.** We use money fields only to check for nonsense (negative
   fares, totals that don't add up). They're two booleans on the trip, not a revenue model — because
   we can't build an honest one (L1, L5).
2. **Rejected rows live in their own table.** Deleting them would erase the audit trail; mixing them
   in with a flag would mean every query must remember to filter, and one forgotten `WHERE` would
   quietly corrupt a number. Two tables, one filter applied once.
3. **Location is read, not guessed.** The dataset we used earlier had only coordinates, so we
   *inferred* zones with a point-in-polygon join. The 2023 data ships TLC's own zone IDs, so the
   inference — and its code — were removed rather than left to rot. (Noted here so a reviewer who
   saw the earlier version knows where it went.)
4. **Placeholder zones are labelled, not deleted or filled in.** TLC uses zone 264 = "Unknown" and
   265 = "NA" when a GPS point didn't resolve — 1% of trips. We keep the value and report those
   trips as their own `zone_unknown` group, so a reader sees "1% have no known location" instead of
   it vanishing or being invented.

## 7. Will it keep working?

| Stage | What it does | If it breaks |
|---|---|---|
| retrieve | pull 12 months from the API, page by page, verify counts | retries 5×; an unverifiable month → exit 2 |
| ingest | fingerprint what we retrieved (sha256, row counts) | no months → exit 2 |
| validate | run the 16 rules, split valid vs quarantined, decide the gate | schema mismatch or broken row counts → **exit 3** |
| transform | build the fact table (calculated columns + zone IDs) | missing input → exit 2 |
| metrics | compute M1–M5, the ruler, hourly/zone/segment cuts | missing input → exit 2 |
| evidence | render the results table from published files | missing file → loud failure |

Exit codes: `0` done · `1` a step failed (traceback in the log and the run manifest) · `3` the
contract aborted · `4` gate says HOLD.

**Reruns are safe — measured, not claimed.** We wiped every derived file and rebuilt: all six metric
outputs came back **byte-identical**, and two full runs agree. That holds because the retrieved data
is never modified, every output is rewritten from scratch, sorted outputs carry an explicit sort
order (this fixed a real non-determinism bug), the metrics are scoped to exactly the months the
validation report covers, and the run manifest is written atomically.

**When things go wrong:**

| Situation | What happens |
|---|---|
| A column the contract needs is missing | **abort, exit 3** — no half-finished numbers published |
| Input so broken that coverage collapses | run finishes but the gate says **HOLD** and explains why |
| API timeout / rate limit | 5 retries with backoff, then a loud failure |
| The API count query times out | recorded as **UNKNOWN** — never 0, never a pass |
| Paging silently repeated or skipped rows | the duplicate audit catches it and fails the gate |
| A single dirty cell in a row | never crashes the job — it becomes a counted failure with a reason |
| A rule can't be evaluated (missing value) | counts as **failed** (the bug we fixed) |

**Tests: 42, no network needed** — each rule fires on a purpose-built row, missing values can't
quietly pass, thresholds map correctly to PASS/WARN/FAIL, and the retrieval logic is tested offline
(paging, resuming, atomic writes, count mismatches, duplicate detection), plus a small end-to-end
run that proves the raw inputs are never modified.

## 8. What we still don't know

Everything here is in the repo, with an owner attached.

**Known (with evidence):** exact per-month row counts reconciled against the API (12/12) · raw pages
preserved and fingerprinted · 97.97% of rows valid, nothing deleted · 15 PASS / 1 WARN / 0 FAIL /
6 UNKNOWN · byte-identical reruns · the baseline month scores 80.02% ≈ 80% · the year-on-year
decline is not a mix artefact (≤0.15 pts) · location is TLC's own zone ID, no missing values.

**Open questions for the client** — we flag them, we never quietly decide them:

| # | Question | Owner | Blocks publishing? |
|---|---|---|---|
| U1 | `ratecode = 99` isn't defined in the TLC guide (213,480 rows) | client / TLC | no — flagged, kept |
| U2 | Passenger counts of 0 or 7–9 (583,414 rows) | client | no — flagged, kept |
| U3 | 774,434 zero-distance trips — meter fault, repositioning, or a cancelled trip billed as a fare? | client | partly — excluded from the KPI for safety |
| U4 | Vendor 6 exists (8,668 rows) but isn't in the guide | client / TLC | no — accepted and flagged |
| U5 | Why do 1,309,356 rows miss rate code, passenger count, store-and-forward and the two new surcharges? | client / TLC | no — kept as real trips, flagged |
| U6 | Are the 98 rows dated outside 2023 (excluded by our monthly windows) expected? | us (documented) | no |

**Assumptions we encoded** (each in `config.yaml` with an owner):

| # | Assumption | Owner |
|---|---|---|
| A1 | One row = one trip; enforced by row conservation + duplicate audit, since there's no trip ID | us / data lead |
| A2 | The API's lowercase columns, quoted headers and parquet re-typing are presentation, not meaning | us |
| A3 | Trips over 24 hours are meter errors | **client sign-off** |
| A4 | Expected time = 80th percentile per distance × hour, from Jan 2023, not season-adjusted | **client sign-off** |
| A5 | 98 rows outside 2023 are out of scope by construction | us |
| A6 | The 3.4% missing-metadata cohort are real trips → kept and flagged | **client confirm** |
| A7 | A missing surcharge counts as 0 for the money check only | us / client |

**Limits no rule can fix:**

- **L1** TLC didn't create this data and disclaims accuracy → absolute values carry unquantifiable
  error, so we publish rates and movement, not "truth".
- **L2** No coordinates in this source; location is zone-level and ~1% of trips are "Unknown".
- **L3** No driver, traffic or weather data → associations only, never causes.
- **L4** Seasonality is visible across 12 months but not adjusted for, so month-to-month movement
  mixes real change with weather.
- **L5** Money fields are unreliable → no money metric is published. The KPI is about time, which is
  what this data can support.

**Before this KPI is unconditionally publishable:** the client answers U1–U5, signs off A3, A4, A6
and A7, and the gate still says PUBLISH afterwards.

## 9. Running it

```bash
# 1. environment (conda; pip fallback: pip install -r requirements.txt)
conda env create -f environment.yml && conda activate fde

# 2. download the year (~13 min, resumable, keeps every raw page)
make fetch

# 3. rebuild every number (~23 s)
make run

# 4. evidence, tests, notebooks
make evidence     # regenerate output/evidence_table.md
make test         # 42 tests
make notebooks    # re-execute the 3 notebooks
```

**From an empty clone:** `make fetch && make run && make evidence && make test && make notebooks`
— about 20 minutes, almost all of it the download.

Useful variations: `make validate` · `make metrics` · `python -m nyc_pipeline.run --months 2023-03`
(one month) · `--refresh-api` (force re-download) · `--require-publish` (exit 4 if the gate says
HOLD) · `make clean` (delete derived files only — the retrieved data and manifest are untouched).

## 10. What's in the repo

```
README.md                 this file
output/                   results: metrics CSVs, validation report, run manifest, ruler,
                          zone/segment detail, evidence table, 2 figures
src/nyc_pipeline/         api_fetch (download) · ingest (fingerprint) · rules + config.yaml
                          (the contract) · validate · transform · metrics · evidence · run
notebooks/                3 evidence notebooks (already executed, outputs included)
tests/                    rule tests · offline download tests · small end-to-end test
data/manifest.json        per-month SHA-256 + where each month came from
data/reference/           zone names, zone shapes (for the map), the download report
logs/last_run.log         the most recent pipeline run
Makefile · environment.yml · requirements.txt · .gitignore
```

**Not in git, on purpose:** the retrieved trip data (499 MB), the raw API pages (694 MB), and the
derived tables (1.3 GB) — 2.5 GB in total. All of it comes back with `make fetch && make run`, and
GitHub rejects files over 100 MB anyway. **That's the 4.8 MB figure: the code, the results, and the
proof — not the data.**

## 11. The 3 notebooks

Read-only evidence, already executed; **every cell has a one-line description above it**, so you can
read them without running anything.

| # | Notebook | What it answers |
|---|---|---|
| 1 | `01_sources_and_retrieval.ipynb` | *Can we trust that we have ALL the data?* — source map, per-month fingerprints, the count reconciliation, the zone cross-check |
| 2 | `02_profile_and_validation.ipynb` | *Can we trust what's IN the data?* — schema, value domains, the 3.4% cohort, the money-residual finding, all 16 rules, the row-conservation and integrity checks, the gate |
| 3 | `03_workflow_and_metrics.ipynb` | *How do trips become a KPI?* — one trip end-to-end, the 30-cell ruler, 12 months of metrics, the real-vs-mix check, worst hours and zones, the map, and what we can't claim |
