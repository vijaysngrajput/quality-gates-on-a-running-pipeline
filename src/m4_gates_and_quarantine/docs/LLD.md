# LLD: Milestone 4, Gates and quarantine

**Status:** Final; built
**Fits inside:** [HLD](HLD.md)

> **What this document is.** A low-level design (LLD) gives the exact detail of each part named in the [HLD](HLD.md): the files, every test with what a failure does, the tables a gate writes, the commands and the tests. Keep it open beside the notebooks and the code. Section 12 says which notebook and which file build each part.

## 1. Files

Everything is inside `src/m4_gates_and_quarantine/`. Nothing imports from another milestone.

```
docs/                     BRD.md, HLD.md, LLD.md
notebooks/                the lab: eight notebooks, run in order
dbt_project/              new: every rule, as a dbt test
  dbt_project.yml         the project's settings: keep failing rows, in the schema `audit`
  profiles.yml            how dbt reaches Postgres, or DuckDB
  models/sources.yml      the tables the tests read, and four generic tests
  models/prepared/        data that must be prepared before it can be checked, with unit tests
  macros/                 one generic test of our own: unique_together
  tests/landing/          2 tests, run when the files have landed
  tests/clean/            35 tests, run after cleaning
  tests/publish/          5 tests, run before the outputs are published
lotus_pipeline/           production code
  dbt_runner.py           new: start dbt from Python, and read what it found
  gates.py                new: run one gate, and act on each failed test
  publish.py              changed: write to staging, then publish
  report.py               changed: pay, or do not pay yet, and why
  clean.py                changed: leaves out refused files. A sales line keeps its file's day
  pipeline.py             changed: eight steps
  contract.py, by_hand.py, consumers.py, profile.py, load.py   as before
  flow_data.py            builds the numbers shown on the flow page and the recap page
contracts/, checks/       the contract, and the seven cards. Question 7 is now answered
dags/lotus_nightly_dag.py     eight tasks. The DAG is lotus_nightly_m4
dags/sql_check_example_dag.py a lab: one check with Airflow's own SQL check operator
tests/                    tests for the production code and for the dbt project
```

Gone: `checks.py`, `sales_checks.py`, `till_checks.py`, `other_checks.py`, `inspect.py`. Their rules are now files in `dbt_project/tests/`. A rule lives in one place.

## 2. The eight steps

| Step | Command | What it does | It can end with exit code 1 |
|---|---|---|---|
| Load | `python -m lotus_pipeline load --day D` | Copies new files to raw, as before | No |
| Landing gate | `... gate-landing --day D` | Tests the files' columns and the supplier's contract. Lists refused files | No. It refuses, it does not block |
| Clean | `... clean --day D` | Rebuilds the clean tables, without the rows of refused files | No |
| Clean gate | `... gate-clean --day D` | 39 tests. Sets rows aside. | Yes: a broken reference table |
| Build | `... build --day D` | **Write:** builds tonight's outputs in the schema `staging` | No |
| Publish gate | `... gate-publish --day D` | **Audit:** builds the status of every store's day, and tests staging | Yes: a difference nobody can explain |
| Publish | `... publish --day D` | **Publish:** copies staging over the tables people read, in one transaction | No |
| Report | `... report --day D` | Prints the bonus report | No |

`python -m lotus_pipeline run --day D` runs all eight and stops at the first that fails.

## 3. A dbt test

One rule, one file. This is `dbt_project/tests/clean/refund_gives_money_back.sql`:

```sql
-- A refund gives money back
--
-- Prepare first: Read sales and refunds apart
-- The limit:     A refund has a quantity and an amount below zero. A sale has both above zero
-- When it fails: QUARANTINE: the rows this test returns are set aside with this reason. The good rows carry on.
{{ config(tags=['clean'], severity='warn', meta={'number': 6, 'action': 'quarantine', 'lens': 'business',
            'question': 'Valid?', 'protects': 'Store managers ...', 'sets_aside': 'sales_lines'}) }}

SELECT line_id, store_id, sales_date, line_type, quantity, amount_cents
FROM {{ source('clean', 'sales_lines') }}
WHERE (line_type = 'refund' AND (amount_cents >= 0 OR quantity >= 0))
   OR (line_type = 'sale' AND (amount_cents <= 0 OR quantity <= 0))
```

| Part | Meaning |
|---|---|
| The first line | The rule, in words. It becomes the reason on a row that is set aside |
| `tags` | The gate that runs this test. It is also the name of the folder |
| `severity` | `error` for a test that blocks. `warn` for every other |
| `meta.action` | What a failure does: `block`, `quarantine`, `refuse_file` or `warn` |
| `meta.sets_aside` | For a quarantine test: the table whose rows are set aside |
| `source('clean', 'sales_lines')` | The table `clean.sales_lines`. dbt did not build it, so it is called a source |

| Kind of test | What it is | Here |
|---|---|---|
| **Generic** | Ready-made, switched on with one line in a YAML file | `not_null`, `unique`, `accepted_values` on `sales_lines`. And our own `unique_together`, written once in `macros/` |
| **Singular** | One query in one file, for one rule | Every file in `tests/` |
| **Unit** | A few rows typed by hand, with the answer. It tests the SQL, not tonight's data | Two, in `models/prepared/prepared.yml` |

## 4. The tests at each gate

### 4.1 `gate_landing`: 3 tests

| Test | The rule | Lens | When it fails |
|---|---|---|---|
| `file_is_missing_a_column` | A file has every column we expect | Data | Refuse file |
| `supplier_contract` | A supplier's file keeps the contract (checked by `contract.py`, not by dbt) | Business | Refuse file |
| `file_has_a_new_column` | A file has no column we did not expect | Data | Warn |

### 4.2 `gate_clean`: 39 tests

| Test | The rule | Lens | When it fails |
|---|---|---|---|
| `one_total_per_till_per_day` | A till has one total for a day (generic test, in sources.yml) | Data | Block |
| `till_adds_up` | A till's total adds up | Business | Block |
| `one_price_at_a_time` | A product has one price on any day | Data | Block |
| `no_gap_between_prices` | A product on sale has a price on every day | Data | Block |
| `product_values_allowed` | A product row makes sense | Data | Block |
| `six_stores_once` | There are six stores, each once | Data | Block |
| `store_values_allowed` | A store row makes sense | Data | Block |
| `every_store_is_known` | Every file is from a store we know | Data | Block |
| `standard_table_is_not_empty` | Standard check 1 of milestone 1: the table is not empty | Data | Block |
| `standard_key_is_not_null` | Standard check 2: the key is not null (generic test) | Data | Block |
| `standard_key_is_unique` | Standard check 3: the key is unique (generic test) | Data | Block |
| `standard_status_is_allowed` | Standard check 4: the status is an allowed value (generic test) | Data | Block |
| `standard_total_is_positive` | Standard check 5 of milestone 1: the total is positive | Data | Block |
| `required_cells` | A cell that must be filled is filled | Data | Quarantine |
| `true_key_unique` | One item line is stored once | Data | Quarantine |
| `right_shape` | A receipt id has the right shape | Data | Quarantine |
| `line_adds_up` | A line adds up | Data | Quarantine |
| `every_sale_has_a_product` | Every sale is for a product that exists | Data | Quarantine |
| `refund_gives_money_back` | A refund gives money back | Business | Quarantine |
| `refund_not_larger_than_sale` | A refund is not larger than the sale | Business | Quarantine |
| `inside_opening_hours` | Nothing is sold while the store is closed | Business | Quarantine |
| `believable_quantity` | A weight is a believable weight | Business | Quarantine |
| `count_values_allowed` | A count is a believable count | Data | Quarantine |
| `cost_is_filled` | Every supplier row has a cost | Data | Quarantine |
| `cost_near_the_last_file` | A cost is near the cost in the last file | Business | Quarantine |
| `cost_below_the_shelf_price` | A product costs less than it sells for | Business | Quarantine |
| `supplier_row_fits_the_product` | A supplier row fits its product | Data | Quarantine |
| `one_person_once` | One person is one member | Data | Quarantine |
| `email_has_the_right_shape` | An email has the shape of an email | Data | Quarantine |
| `member_cells_filled` | A member row has what marketing needs | Data | Quarantine |
| `price_of_the_day` | The price charged is the price valid that day | Business | Warn |
| `normal_volume` | A store's day is near its normal day | Operational | Warn |
| `every_till_reported` | Every till of an open store sent its total | Operational | Warn |
| `price_change_is_believable` | A new price is near the old price | Business | Warn |
| `retired_means_ended` | A retired product has a last day | Business | Warn |
| `stores_did_not_change` | The stores table is the same as yesterday | Operational | Warn |
| `a_count_for_every_day` | A snapshot has a row for every day | Data | Warn |
| `one_count_per_day` | A product is counted once a day | Data | Warn |
| `member_fits_the_real_world` | A member has a real store and a past join date | Business | Warn |

### 4.3 `gate_publish`: 5 tests

| Test | The rule | Lens | When it fails |
|---|---|---|---|
| `unexplained_difference` | The receipts add up to the till's own total, or we can say why not | Business | Block |
| `one_row_per_store_per_day` | The table the bonus is paid from has one row per store per day | Data | Block |
| `store_is_not_ready_to_pay` | Every open store has a day we can pay from | Operational | Warn |
| `order_without_a_cost` | Every order on the list has a cost | Business | Warn |
| `order_without_a_count` | Every order on the list is worked out from a real count | Operational | Warn |

Two checks of milestone 2 have a new home. "The receipts add up to the till's own total" and "every open store reported" are no longer row tests. They are the model `store_day_status`, and the two publish tests that read it.

## 5. The prepared models (`models/prepared/`)

Built by dbt as tables in the schema `prepared`, every night, by the publish gate.

| Model | One row is | Used by |
|---|---|---|
| `expected_columns` | One column a kind of file must have | The two landing tests |
| `expected_store_days` | One open store on one loaded day | `store_day_status` |
| `till_reconciliation` | One till on one day: the till's total, and what its lines add up to | `store_day_status` |
| `store_day_status` | One open store on one day: can it be paid from, and if not, why | The publish gate, and the bonus report |

`store_day_status.problem`, in the order the model asks:

| `problem` | When | `status` |
|---|---|---|
| empty | The lines that are left add up to the tills' total | `ok` |
| `no_till_total` | There is no till total to compare with | `do not pay yet` |
| `file_refused` | The landing gate refused the store's file | `do not pay yet` |
| `no_sales_file` | No lines, and nothing set aside | `do not pay yet` |
| `rows_set_aside` | The day does not add up, and rows of that file were set aside | `do not pay yet` |
| `unexplained` | The day does not add up, and nothing above explains it | `do not pay yet`, and the publish gate **blocks** |

Two unit tests prove the SQL on rows typed by hand: a till with no lines counts as zero, and one store for each of the answers above.

```sh
uv run python -m lotus_pipeline dbt test --select test_type:unit
```

## 6. What a gate writes

| Table | Written by | Holds |
|---|---|---|
| `audit.<test name>` | dbt | The rows each test returned tonight. One table per test |
| `ops.refused_files` | `gate_landing` | One row per refused file: the file, the test, the reason, the night |
| `quarantine.sales_lines` | `gate_clean` | Every column of the sales line, plus `check_key`, `reason`, `is_a_copy`, `set_aside_on`. A line with two reasons has two rows |
| `quarantine.raw_sales_lines` | `gate_clean` | Sales lines that never reached the clean table: no product |
| `quarantine.supplier_prices`, `quarantine.stock_counts` | `gate_clean` | The row, with the reason |
| `quarantine.members` | `gate_clean` | `member_id`, `check_key`, `reason`, `set_aside_on`. **No name, no email, no phone** |
| `ops.gate_results` | Every gate | One row per test per night: its action, pass or fail, how many rows |
| `prepared.store_day_status` | `gate_publish` | See section 5 |

`ops.refused_files` is a **dead-letter table**: a place for things that could not be processed at all. A quarantine table holds rows. This holds whole files.

The clean tables are rebuilt from raw every night, and so is the quarantine. Nothing is remembered from the night before: every row is judged again. When a store sends a corrected file, the row comes back by itself.

### The 5% limit

After the row tests, one more rule: if more than 5% of the lines of one store's file were set aside, the rest of that file is set aside too, with the reason "More than 5% of this file was set aside, so the whole file is refused". Extra copies do not count: removing a copy is safe.

## 7. Write, audit, publish (`publish.py`)

| Function | Does |
|---|---|
| `build(wh, day)` | Builds `staging.daily_store_sales` and `staging.replenishment_list` |
| `publish(wh, day)` | Copies them to `output`, and `prepared.store_day_status` to `output.store_day_status` |

One rule of the given pipeline is changed. In milestone 1 a missing stock count was read as zero. Now:

```sql
stock.counted_qty                          AS stock_counted,   -- empty when nobody counted
CASE WHEN stock.counted_qty IS NULL THEN NULL
     ELSE greatest(2 * coalesce(sold.quantity, 0) - stock.counted_qty, 0) END AS order_qty,
CASE WHEN stock.counted_qty IS NULL THEN 'no count: order by hand' END AS note
```

## 8. The bonus report (`report.py`)

After the seven nights of the bonus week:

```
  Store                           Manager              Net sales  Bonus (1%)   Pay?
  S01  Lotus Ashby High Street    Marta Lindqvist      $5,153.25      $51.53   yes
  S02  Lotus Ashby Market Square  Dana Okafor          $5,724.68           -   DO NOT PAY YET
         Sun 08-30: 4 rows set aside ($23.95): Nothing is sold while the store is closed
  S03  Lotus Calder Bridge        Tomas Reyes          $5,097.11           -   DO NOT PAY YET
         Mon 08-24: 1 row set aside ($9.49): A line adds up; A refund gives money back
  S04  Lotus Calder Station       Ines Marchetti       $4,585.57           -   DO NOT PAY YET
         Tue 08-25: no sales file arrived. The tills counted $587.42
  S05  Lotus Fenwick Green        Samir Haddad         $3,591.53           -   DO NOT PAY YET
         Thu 08-27: 1 row set aside ($1,522.20): A weight is a believable weight
  S06  Lotus Fenwick Harbour      Aiko Tanaka          $4,400.92           -   DO NOT PAY YET
         Sat 08-29: 3 rows set aside ($17.97): Every sale is for a product that exists

  1 of 6 stores can be paid.
```

A store is paid when every one of its days in the week has the status `ok`.

What happened to each planted case:

| Case | Gate | What it did | The result |
|---|---|---|---|
| 1. A refund as a positive amount | `gate_clean` | Set 1 row aside | `S03` waits. $9.49 is in doubt |
| 2. One receipt twice | `gate_clean` | Set 4 extra copies aside | `S01`'s day is right again. **Pay** |
| 3. A weight in grams | `gate_clean` | Set 1 row aside | `S05` waits |
| 4. Yesterday's price | `gate_clean` | Warned, 6 lines | `S04` can be paid for that day. Finance is owed $6.00 |
| 5. A sale at 03:10 | `gate_clean` | Set 4 rows aside | `S02` waits. $23.95 |
| 6. A product nobody typed | `gate_clean` | Recorded 3 raw rows | `S06` waits. $17.97 |
| 7. No file | `gate_publish` | Warned: a partial load | `S04` waits. The tills counted $587.42 |
| 8. A late file | `gate_publish` | Warned for one night | `S06` waited one night, and was right the next |
| 9. The whole file twice | `gate_clean` | Set 167 extra copies aside | `S02`'s Friday is right again |
| 10. A renamed column | `gate_landing` | Refused the file | The last good costs are used |
| 11. Cost in cents | `gate_landing` | Refused the file, by the contract | The last good costs are used |
| 12. A missing stock count | `gate_clean` | Warned | The order says "no count: order by hand" |
| 13. The same member twice | `gate_clean` | Set the newer member aside | Marketing has one member, and one email |

## 9. The DAG (`dags/lotus_nightly_dag.py`)

```
stores_send_files >> load >> gate_landing >> clean >> gate_clean >> build >> gate_publish >> publish
```

| Setting | Value | Why |
|---|---|---|
| `dag_id` | `lotus_nightly_m4` | One DAG per milestone |
| Retries | 0 | A test that failed on the data will fail again. A person must look |
| A gate task | `python -m lotus_pipeline gate-clean --day {{ ds }}` | A plain command. Airflow holds no rule |

When a gate ends with exit code 1, Airflow gives the tasks these states:

| Task | State |
|---|---|
| The gate | `failed` |
| Every task after it | `upstream_failed`: a task before it failed, so it was not started |

Inside Docker, dbt writes its working files to `/tmp`, set in `docker-compose.yml`.

## 10. One check, three ways, and a fourth

The check: `line_type` is `sale` or `refund`.

| Written | It looks like |
|---|---|
| By hand | `SELECT * FROM clean.sales_lines WHERE line_type NOT IN ('sale', 'refund')` |
| dbt | `- accepted_values:` with `values: ["sale", "refund"]`, under the column in `sources.yml` |
| Great Expectations | `ExpectColumnValuesToBeInSet(column="line_type", value_set=["sale", "refund"])` |
| Soda | `- invalid_count(line_type) = 0:` with `valid values: [sale, refund]` |

Great Expectations and Soda are not installed here. The two lines show how the check would look. The tool changes. The question, who is hurt when `line_type` is something else, does not.

A fourth shape is in `dags/sql_check_example_dag.py`: Airflow's own `SQLCheckOperator`. It runs a query that returns one row, and passes when that row is true.

| | A dbt test | `SQLCheckOperator` |
|---|---|---|
| The query returns | The bad rows | One true-or-false row |
| It passes when | No row comes back | The row is true |
| The rule lives in | The dbt project | The DAG file |
| The bad rows | Can be kept, and set aside | Are not known: only pass or fail |

## 11. Tests

| Test file | What it proves |
|---|---|
| `test_gates.py` | **Test the tests.** Four clean weeks pass every gate, and all six stores can be paid. In the bonus week each planted case is caught by its gate and handled as section 8 says. What is published is the truth less exactly what was set aside. A deleted store and a second price block at the clean gate. A valid row with a wrong quantity blocks at the publish gate. A blocked run leaves the published table unchanged. More than 5% bad rows refuses the file. The week runs on DuckDB and on Postgres |
| `test_dbt_project.py` | Every test file states its rule and what a failure does. Only a block is an `error`. No test on members selects personal data. The unit tests pass |
| `test_dag.py` | Eight tasks, a gate in three places, each task a plain command, no SQL in the DAG. The real Airflow reads both DAG files |
| `test_report.py` | Each reason a store cannot be paid is one plain line |
| `test_load.py`, `test_contract.py`, `test_by_hand.py`, `test_consumers.py`, `test_profile.py` | The code copied forward still works |
| `test_notebook_parity.py` | What the notebooks saved equals what the pipeline produced |
| `test_flow_page.py` | Every number on the flow page and the recap page comes from the code |

## 12. Where each part is built

| Notebook | Teaches | Design sections | Code |
|---|---|---|---|
| `01_a_rule_lives_in_dbt` | Generic test, singular test | HLD 2; LLD 3 | `dbt_project/` |
| `02_how_serious_is_it` | Severity, stored failures | LLD 3, 6 | `dbt_project.yml` |
| `03_test_the_sql_not_the_data` | Unit test | LLD 5 | `models/prepared/` |
| `04_a_gate_and_where_it_stands` | Gate, gate placement | HLD 1, 3; LLD 2, 4 | `gates.py` |
| `05_warn_block_or_set_aside` | Warn or block; write, audit, publish; upstream failure | BRD 3, 4; HLD 4, 5; LLD 7, 9 | `publish.py`, `dags/` |
| `06_quarantine` | Quarantine, rejected rows, dead-letter table, partial load | BRD 5; HLD 6; LLD 6, 8 | `gates.py`, `report.py` |
| `07_test_the_tests` | | LLD 10, 11 | `tests/`, `dags/sql_check_example_dag.py` |
| `08_the_production_version` | The whole week, under Airflow | LLD 8, 9 | `pipeline.py` |

## 13. What is still wrong

| What we have | What is missing |
|---|---|
| A report that is right, and says which store to hold back and why | A person who reads it before Monday |
| `S04` marked "no sales file arrived" since Tuesday night | Someone told on Tuesday night, while the store could still send it |
| A gate that can fail | Anyone who notices |

**A gate fails at 02:00, and nobody knows until a store manager complains.** Milestone 5 tells the right person, and recovers.
