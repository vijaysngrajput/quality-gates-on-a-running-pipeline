# LLD: Milestone 2, Checks with a reason

**Status:** Final; built
**Fits inside:** [HLD](HLD.md)

> **What this document is.** A low-level design (LLD) gives the exact detail of each part named in the [HLD](HLD.md): the files, the SQL of every check, its limit, and what a failure does. Keep it open beside the notebooks and the code. Section 9 says which notebook and which file build each part.

## 1. Files

Everything is inside `src/m2_checks_with_a_reason/`. Nothing imports from another milestone. The pipeline is copied forward from milestone 1.

```
docs/                     BRD.md, HLD.md, LLD.md
notebooks/                the lab: eight notebooks, run in order
lotus_pipeline/           production code
  load.py, clean.py, checks.py, publish.py, report.py, pipeline.py   the pipeline, as before
  by_hand.py, consumers.py, profile.py                               from milestone 1
  reference/calendar.csv  new: the days that are not normal
  sales_checks.py         new: 13 checks for sales_lines
  till_checks.py          new: 3 checks for till_totals
  inspect.py              new: run the checks, and say what each one found
  flow_data.py            builds the numbers shown on the flow page and the recap page
checks/                   sales_lines.md (complete), till_totals.md (new)
dags/lotus_nightly_dag.py the same five tasks. The DAG is lotus_nightly_m2
tests/                    one test file per code file
milestone_2_flow.html     the whole pipeline, animated
milestone_2_recap.html    the closing page
```

Two small changes to the given code:

| File | Change |
|---|---|
| `checks.py` | A check can now say why it exists: `lens`, `question`, `protects`, `prepare`, `limit`, `catches` |
| `clean.py` | The clean step also builds `clean.calendar` from `reference/calendar.csv` |

## 2. What a check is made of

```python
Check(
    number=11,
    key="till_total",
    name="The receipts add up to the till's own total",
    table="clean.sales_lines",
    lens="business",  # business, data or operational
    question="Accurate?",  # which of the six questions it answers
    protects="Store managers ...",  # who is hurt when it fails
    prepare="Add the lines up per till per day. Join that to till_totals ...",
    limit="The two totals are equal, to the cent ...",
    catches=(1, 2, 3, 5, 6, 7, 9),  # the planted cases it catches
    sql="...",  # returns the bad rows
)
```

A check reads the whole table. It takes no day. It returns one row per bad thing, always with `store_id` and `sales_date`, so every finding can be said as "this store, this day".

## 3. The calendar (`clean.calendar`)

| Column | Type | Example |
|---|---|---|
| `calendar_date` | DATE | `2026-08-26` |
| `store_id` | TEXT | `S03`. A row for `ALL` in the file becomes one row per store |
| `kind` | TEXT | `public_holiday` or `closed` |
| `opens_at`, `closes_at` | TIME | `10:00`, `16:00`. Empty when closed |
| `note` | TEXT | `Founders' Day` |

Seven rows: six for the holiday, one for the closed store.

## 4. The checks for `sales_lines` (`sales_checks.py`)

What each finds after the seven nights of the bonus week. A failure does nothing yet: `inspect` only reports.

| # | Key | Lens | Question | Bad rows | Found |
|---|---|---|---|---|---|
| 1 | `required_cells` | Data | Complete? | 0 | Nothing. Its fault is planted in a test |
| 2 | `true_key_unique` | Data | Unique? | 171 | Case 2: 4 lines at `S01`. Case 9: 167 lines at `S02` |
| 3 | `right_shape` | Data | Valid? | 0 | Nothing. Its fault is planted in a test |
| 4 | `line_adds_up` | Data | Consistent? | 1 | Case 1 |
| 5 | `every_sale_has_a_product` | Data | Consistent? | 3 | Case 6: $17.97 |
| 6 | `refund_gives_money_back` | Business | Valid? | 1 | Case 1 |
| 7 | `refund_not_larger_than_sale` | Business | Consistent? | 0 | Nothing. Its fault is planted in a test |
| 8 | `price_of_the_day` | Business | Accurate? | 6 | Case 4: the store is $6.00 short |
| 9 | `inside_opening_hours` | Business | Valid? | 4 | Case 5: $23.95 |
| 10 | `believable_quantity` | Business | Valid? | 1 | Case 3: 1,180 kg of bananas |
| 11 | `till_total` | Business | Accurate? | 11 | Cases 1, 2, 3, 5, 6, 7 and 9 |
| 12 | `every_store_reported` | Operational | Complete? | 1 | Case 7. And case 8, on its own night |
| 13 | `normal_volume` | Operational | Complete? | 1 | Case 9: 226% of a normal Friday |

### 4.1 The common version, and the careful one

| Common check | What it misses | Careful check |
|---|---|---|
| `line_id` is not null | An empty amount. A sum ignores an empty cell, and nothing fails | 1: say which columns must be filled, and in which rows |
| `member_id` is not null | Nothing. It fails on 65% of good rows | No check at all. Nobody is hurt by an empty `member_id` |
| `line_id` is unique | A receipt or a file sent twice. Every `line_id` is new | 2: unique on `receipt_id` + `line_no` |
| `quantity` is above zero | It fails on every good refund | 10: a limit per unit |
| `amount_cents` is below a large number | It fires on the real sale of 400 bottles | 10: limit the weight, not the money |
| The table is not empty | One store of six sending nothing | 12: every open store, every day |
| At least 100 lines a day | It fails `S05` on every weekday | 13: each store against its own normal day |

### 4.2 The SQL that needs explaining

**Check 2, one item line is stored once.** Grouped by the true key.

```sql
SELECT receipt_id, line_no, store_id, min(sales_date) AS sales_date, count(*) AS times,
       sum(amount_cents) - min(amount_cents) AS counted_too_much_cents
FROM clean.sales_lines
GROUP BY receipt_id, line_no, store_id
HAVING count(*) > 1
```

**Check 5, every sale is for a product that exists.** It reads the **raw** rows. The clean table cannot be asked: the rows are no longer there.

```sql
SELECT l.line_id, l.store_id, ..., l.product_id, CAST(l.amount_cents AS BIGINT) AS lost_cents
FROM raw.sales_lines l
LEFT JOIN clean.products p
  ON p.product_id = l.product_id
 AND CAST(CAST(l.sold_at AS TIMESTAMP) AS DATE)
     BETWEEN p.valid_from AND coalesce(p.valid_to, DATE '9999-12-31')
LEFT JOIN clean.stores s ON s.store_id = l.store_id
WHERE p.product_id IS NULL OR s.store_id IS NULL
```

**Check 9, nothing is sold while the store is closed.** The calendar's hours win over the store's own.

```sql
WHERE CAST(l.sold_at AS TIME) < coalesce(c.opens_at, s.opens_at)
   OR CAST(l.sold_at AS TIME) > coalesce(c.closes_at, s.closes_at)
```

**Check 11, the receipts add up to the till's own total.** Two halves. The first starts from the tills, so a till with no lines fails. The second starts from the lines, so lines with no till fail.

```sql
SELECT t.store_id, t.till_id, t.business_date AS sales_date,
       t.net_total_cents AS till_cents, coalesce(l.lines_cents, 0) AS lines_cents,
       coalesce(l.lines_cents, 0) - t.net_total_cents AS difference_cents
FROM clean.till_totals t
LEFT JOIN (
    SELECT store_id, till_id, sales_date, sum(amount_cents) AS lines_cents
    FROM clean.sales_lines GROUP BY store_id, till_id, sales_date
) l ON l.store_id = t.store_id AND l.till_id = t.till_id AND l.sales_date = t.business_date
WHERE coalesce(l.lines_cents, 0) <> t.net_total_cents
UNION ALL
SELECT ... lines that have no row in till_totals ...
```

What it returns for the bonus week:

| Store, till, day | Tills | Lines | Difference | Case |
|---|---|---|---|---|
| `S03` `T1` 08-24 | $419.59 | $438.57 | +$18.98 | 1 |
| `S01` `T2` 08-25 | | | +$18.07 | 2 |
| `S04` `T1` and `T2` 08-25 | $587.42 together | $0.00 | −$587.42 | 7 |
| `S05` `T1` 08-27 | | | +$1,520.68 | 3 |
| `S02` `T1` and `T2` 08-28 | $1,034.28 together | twice that | +$1,034.28 | 9 |
| `S06` `T1` and `T2` 08-29 | | | −$17.97 together | 6 |
| `S02` `T2` 08-30 | | | −$23.95 | 5 |
| `S02` `T2` 08-31 | no till | $23.95 | +$23.95 | 5 |

It does not return `S04` on 08-28, case 4. The till charged the same old price, so the two counts agree.

**Check 13, a store's day is near its normal day.** The normal day is built first.

```sql
-- one row per store per day: its lines, and its normal day
SELECT today.store_id, today.sales_date, today.lines,
       count(before.lines) AS days_compared, avg(before.lines) AS normal_lines
FROM (lines per store per day) today
JOIN (lines per store per day) before
  ON before.store_id = today.store_id
 AND before.sales_date IN (today.sales_date - 7, today.sales_date - 14,
                           today.sales_date - 21, today.sales_date - 28)
WHERE NOT EXISTS (a calendar row for either day)
GROUP BY today.store_id, today.sales_date, today.lines
```

| Setting | Value | Why |
|---|---|---|
| The baseline | The average of the same weekday in the four weeks before | A Saturday is not a Tuesday |
| Days compared | At least 3 | Fewer is not a normal day yet |
| The limit | Below 60% or above 140% of the baseline | The clean weeks stay between 74% and 131% |
| Calendar days | Left out on both sides | A holiday is not normal, and is not judged as normal |

## 5. The checks for `till_totals` (`till_checks.py`)

| # | Key | Lens | The limit |
|---|---|---|---|
| 14 | `one_total_per_till_per_day` | Data | Each `store_id` + `till_id` + `business_date` is there once |
| 15 | `till_adds_up` | Business | Net is gross less refunds. No number is below zero |
| 16 | `every_till_reported` | Operational | Two totals per open store per day |

All three return nothing for the five weeks. Their faults are planted in tests.

## 6. `inspect` (`inspect.py`)

```sh
uv run python -m lotus_pipeline inspect                      # every check, and what it found
uv run python -m lotus_pipeline inspect --check till_total   # one check: its reason, and its bad rows
```

| Function | Returns |
|---|---|
| `run_all(wh)` | One `Finding` per check: the check, and its bad rows |
| `run_one(wh, key)` | The same for one check |
| `Finding.store_days()` | The stores and days of the bad rows, each once |
| `format_results(findings)` | The report, grouped by lens |

After the bonus week it ends with:

```
  10 of 16 checks found bad rows. Nothing was stopped:
  these checks are not part of the nightly run yet.
```

## 7. What a failure does

Nothing, in this milestone. That is on purpose, and it is what is still wrong after milestone 3.

| | The five standard checks | The sixteen checks with a reason |
|---|---|---|
| Run by | The `check` task, every night | A person, with `inspect` |
| A failure | Stops `publish` | Prints a line |
| They found, in the bonus week | Nothing | Every planted sales fault |

## 8. Tests

| Test file | What it proves |
|---|---|
| `test_sales_checks.py` | **Every check has two kinds of test.** On four clean weeks each returns nothing. On the bonus week each returns exactly its planted store-days, with the planted amounts. The three checks with no standing fault get one planted on a private copy. No check returns the holiday, the closed store or the 400 bottles. The three checks for `till_totals` are tested the same way |
| `test_pipeline.py`, `test_checks.py`, `test_load.py`, `test_report.py` | The pipeline is unchanged: still five green checks and six wrong stores |
| `test_by_hand.py`, `test_consumers.py`, `test_profile.py`, `test_dag.py` | Milestone 1's code, copied forward, still works |
| `test_notebook_parity.py` | What the notebooks found by hand equals what the production checks find |
| `test_flow_page.py` | Every number on the flow page and the recap page comes from the code |

The whole week is tested on DuckDB, and again on Postgres when the warehouse is running.

## 9. Where each part is built

| Notebook | Teaches | Design sections | Code |
|---|---|---|---|
| `01_empty_in_which_rows` | Not-null check | LLD 4.1 | `sales_checks.py`: check 1 |
| `02_unique_on_which_columns` | Unique check, composite key | HLD 4; LLD 4.2 | Check 2 |
| `03_a_believable_value` | Range check, format check | HLD 6; LLD 4.1 | Checks 3, 4, 10 |
| `04_rules_of_the_business` | Business rule, reference data, validity and accuracy | BRD 4; HLD 3, 5; LLD 3 | Checks 6, 7, 8, 9 |
| `05_across_two_tables` | Reconciliation, control total, referential integrity, orphan row | HLD 4; LLD 4.2, 5 | Checks 5, 11; `till_checks.py` |
| `06_a_normal_day` | Baseline, limit, anomaly | HLD 6; LLD 4.2 | Checks 12, 13 |
| `07_the_false_alarm` | False positive, false negative, seasonality | BRD 5; HLD 5, 7 | `reference/calendar.csv` |
| `08_the_production_version` | The same logic as a tested program | LLD 6, 8 | `inspect.py`, `tests/` |

## 10. What is still wrong

| What we have | What is missing |
|---|---|
| `sales_lines` and `till_totals`, checked with a reason | Five more tables: products, stores, stock counts, supplier prices, members |
| Checks made for a table of money, written by tills | A supplier's file, a hand count, a form typed by a customer. They fail in other ways |

**One table is done well. There are five more, and they are nothing like it.** Milestone 3 does them.
