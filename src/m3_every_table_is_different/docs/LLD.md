# LLD: Milestone 3, Every table is different

**Status:** Final; built
**Fits inside:** [HLD](HLD.md)

> **What this document is.** A low-level design (LLD) gives the exact detail of each part named in the [HLD](HLD.md): the files, every check with its limit, the SQL that needs explaining, the contract, and the tests. Keep it open beside the notebooks and the code. Section 11 says which notebook and which file build each part.

## 1. Files

Everything is inside `src/m3_every_table_is_different/`. Nothing imports from another milestone. The code is copied forward from milestone 2.

```
docs/                     BRD.md, HLD.md, LLD.md
notebooks/                the lab: eight notebooks, run in order
lotus_pipeline/           production code
  ...                     the pipeline, and milestone 2's checks, as before
  other_checks.py         new: 21 checks for the five other tables
  contract.py             new: hold a supplier's file against the contract
  inspect.py              now runs 37 checks on 7 tables, and compares the cards
  load.py                 one addition: raw.file_columns
  flow_data.py            builds the numbers shown on the flow page and the recap page
contracts/supplier_prices.yml   new: the data contract, written by hand
checks/                   seven check cards, one per table
dags/lotus_nightly_dag.py the same five tasks. The DAG is lotus_nightly_m3
tests/                    one test file per code file
milestone_3_flow.html     the whole pipeline, animated
milestone_3_recap.html    the closing page
```

Two small changes to the code that was copied forward:

| File | Change |
|---|---|
| `load.py` | The load step writes one row per column of every file into `raw.file_columns` |
| `checks.py` | A check has a `family`, so the cards of two tables can be compared |

## 2. A bad row, for a table with no store and day

A check for `sales_lines` returns `store_id` and `sales_date`. A price row or a member has neither. So these checks return two other columns:

| Column | Holds | Example |
|---|---|---|
| `thing` | Which product, store, file or member | `P-006`, `supplier_prices_SUP-B_2026-08-25.csv`, `M-0117` |
| `day` | The day it is about | `2026-08-10` |

The checks for `stock_counts` keep `store_id` and `sales_date`: a count belongs to a store and a day.

## 3. `raw.file_columns`

| Column | Example |
|---|---|
| `file_name` | `supplier_prices_SUP-B_2026-08-25.csv` |
| `table_name` | `supplier_prices` |
| `column_name` | `cost_per_unit` |
| `position_in_file` | `3` |

The load step reads a file by column **name**. A column it does not expect is ignored, and a column it expects and does not find becomes empty. After the load, nothing in `raw.supplier_prices` says which of the two happened. This table does.

## 4. `products`: five checks

| # | Key | Lens | Question | The limit | Found in the bonus week |
|---|---|---|---|---|---|
| 17 | `one_price_at_a_time` | Data | Unique? | No two price rows of one product cover the same day | Nothing. Its fault is planted in a test |
| 18 | `no_gap_between_prices` | Data | Complete? | An active price that ends is followed, the next day, by another price | Nothing. Its fault is planted in a test |
| 19 | `product_values_allowed` | Data | Valid? | A name. A price above zero. A unit of each or kg. A status of active or retired. A start that is not after its end | Nothing. Its fault is planted in a test |
| 20 | `price_change_is_believable` | Business | Accurate? | A new price is at most 2 times the old one, and at least half of it | Nothing. Its fault is planted in a test |
| 21 | `retired_means_ended` | Business | Consistent? | status is retired exactly when the last price row has an end date and nothing follows | Nothing. Its fault is planted in a test |

The two that test a row against its neighbours:

```sql
-- 17. No two price rows of one product cover the same day
SELECT a.product_id AS thing, b.valid_from AS day, ...
FROM clean.products a
JOIN clean.products b
  ON b.product_id = a.product_id AND b.valid_from > a.valid_from
 AND b.valid_from <= coalesce(a.valid_to, DATE '9999-12-31')

-- 18. A price that ends is followed, the next day, by another
SELECT a.product_id AS thing, a.valid_to + 1 AS day, ...
FROM clean.products a
LEFT JOIN clean.products b
  ON b.product_id = a.product_id AND b.valid_from = a.valid_to + 1
WHERE a.valid_to IS NOT NULL AND a.status = 'active' AND b.product_id IS NULL
```

What each fault would do to the sales, because the cleaning step joins a sale to the price valid on its day:

| Fault | The join | Net sales |
|---|---|---|
| Two prices on one day | Finds two rows. The sale is there twice | Too high, in every store |
| A day with no price | Finds no row. The sale is dropped | Too low, in every store |

## 5. `stores`: four checks

| # | Key | Lens | Question | The limit | Found in the bonus week |
|---|---|---|---|---|---|
| 22 | `six_stores_once` | Data | Unique? | Exactly 6 rows, and no store_id twice | Nothing. Its fault is planted in a test |
| 23 | `store_values_allowed` | Data | Valid? | A name, a town and a manager. It opens before it closes. It has at least one till | Nothing. Its fault is planted in a test |
| 24 | `every_store_is_known` | Data | Consistent? | Every store_id in the three store files is in the stores table | Nothing. Its fault is planted in a test |
| 25 | `stores_did_not_change` | Operational | Consistent? | No row is new, gone or different. A real change is confirmed by a person | Nothing. Its fault is planted in a test |

```sql
-- 25. The newest file, less the file before it. What is left has changed
SELECT store_id, store_name, town, opens_at, closes_at, manager_name, tills FROM raw.stores
WHERE _file_name = (the newest file)
EXCEPT
SELECT store_id, store_name, town, opens_at, closes_at, manager_name, tills FROM raw.stores
WHERE _file_name = (the file before it)
```

Check 22 knows the number six from `EXPECTED_STORES` in the code. It is written there by a person. The data cannot supply it.

## 6. `stock_counts`: three checks

| # | Key | Lens | Question | The limit | Found in the bonus week |
|---|---|---|---|---|---|
| 26 | `a_count_for_every_day` | Data | Complete? | Every product on sale has a count, in every open store, on every day | Case 12: 1 row. `S05`, Whole milk 2L, 08-28 |
| 27 | `one_count_per_day` | Data | Unique? | Each count_date + store_id + product_id is there exactly once | Nothing. Its fault is planted in a test |
| 28 | `count_values_allowed` | Data | Valid? | Not empty, not below zero. Pieces are whole numbers. Counted on its own day, by scanner or by hand | Nothing. Its fault is planted in a test |

```sql
-- 26. Build the counts a day should have. Then look for the ones that are not there
SELECT e.store_id, e.day AS sales_date, p.product_id, p.product_name, ...
FROM (every open store, on every loaded day) e
JOIN clean.products p
  ON e.day BETWEEN p.valid_from AND coalesce(p.valid_to, DATE '9999-12-31')
LEFT JOIN clean.stock_counts c
  ON c.store_id = e.store_id AND c.product_id = p.product_id AND c.count_date = e.day
WHERE c.product_id IS NULL
```

Planted case 12, in numbers:

| | Sold on 08-28 | On the shelf | Order: 2 × sold − shelf |
|---|---|---|---|
| The truth | 9 | 3 | 15 |
| The pipeline, with no count | 9 | read as 0 | 18 |

## 7. `supplier_prices`: five checks

| # | Key | Lens | Question | The limit | Found in the bonus week |
|---|---|---|---|---|---|
| 29 | `files_have_their_columns` | Data | Valid? | No expected column is missing. A column we did not expect is reported too | Case 10: 2 rows. `unit_cost` missing, `cost_per_unit` new |
| 30 | `cost_is_filled` | Data | Complete? | No empty unit_cost, product, supplier, unit or date | Case 10: 12 rows |
| 31 | `cost_near_the_last_file` | Business | Accurate? | A cost is at most 2 times the last one, and at least half of it | Case 11: 15 rows |
| 32 | `cost_below_the_shelf_price` | Business | Accurate? | unit_cost_cents is below unit_price_cents | Case 11: 15 rows |
| 33 | `supplier_row_fits_the_product` | Data | Consistent? | Each supplier + product + date is there once. The product exists. The unit is the product's unit | Nothing. Its fault is planted in a test |

```sql
-- 29. The columns we expect, beside the columns each file really had
SELECT f.file_name AS thing, f.table_name, e.column_name,
       'an expected column is missing' AS problem, 'breaking' AS kind_of_change
FROM (every file) f
JOIN (the expected columns of its table) e ON e.table_name = f.table_name
LEFT JOIN raw.file_columns found
  ON found.file_name = f.file_name AND found.column_name = e.column_name
WHERE found.column_name IS NULL
UNION ALL
SELECT ... 'a column we did not expect' AS problem, 'additive' AS kind_of_change ...

-- 31. Each cost, beside the same supplier and product in the file before
WHERE new.unit_cost_cents > 2 * old.unit_cost_cents
   OR new.unit_cost_cents * 2 < old.unit_cost_cents
```

Check 29 reads every file of every table. It is listed here because the suppliers' file is the one Lotus does not control.

| Fault | 29 columns | 30 filled | 31 last file | 32 shelf price |
|---|---|---|---|---|
| Case 10, a renamed column | **Fails** | **Fails** | Silent: nothing to compare | Silent |
| Case 11, cents for dollars | Silent: every column is there | Silent: every cost is filled | **Fails** | **Fails** |

## 8. `members`: four checks

| # | Key | Lens | Question | The limit | Found in the bonus week |
|---|---|---|---|---|---|
| 34 | `one_person_once` | Data | Unique? | Each cleaned email belongs to one member_id | Case 13: 1 row. `M-0117` and `M-0304` |
| 35 | `email_has_the_right_shape` | Data | Valid? | One @, with text before it. A dot after it. No space inside | Nothing. Its fault is planted in a test |
| 36 | `member_cells_filled` | Data | Complete? | A name, a home store, a join date, and a yes or no to marketing. A phone may be empty | Nothing. Its fault is planted in a test |
| 37 | `member_fits_the_real_world` | Business | Consistent? | home_store_id is in the stores table. joined_on is not after the newest loaded day | Nothing. Its fault is planted in a test |

```sql
-- 34. Clean the email first. Then group by it
SELECT min(member_id) AS first_member_id, max(member_id) AS second_member_id, count(*) ...
FROM clean.members
GROUP BY lower(trim(email))
HAVING count(*) > 1
```

No check here selects `full_name`, `email` or `phone`. The three names are in `PERSONAL_COLUMNS`, and a test looks for them in every result.

## 9. The contract (`contracts/supplier_prices.yml`, `contract.py`)

| Part of the contract | Holds |
|---|---|
| `owner` | The producer, the consumer, and who to write to |
| `columns` | Five columns, each with a type and a meaning |
| The rules for the values | `allowed`, `shape`, `decimals`, `min`, `max` on a column |
| `change_policy` | An added column is allowed and reported. A renamed or removed column, a new type or a new meaning needs 30 days' notice and a new version |

`check_file(path)` returns every way one file breaks the contract.

| Kind | Meaning | What happens to the file |
|---|---|---|
| `breaking` | An agreed column is missing | Refuse |
| `value` | A value breaks the rule of its column | Refuse |
| `additive` | A column that is not in the contract | Accept, and report |

```sh
uv run python -m lotus_pipeline contract                  # every supplier file that arrived
uv run python -m lotus_pipeline contract --file <path>    # one file
```

For the five weeks: 15 files, 2 refused.

```
  supplier_prices_SUP-B_2026-08-25.csv       refuse
      breaking  unit_cost: an agreed column is missing
      additive  cost_per_unit: a column that is not in the contract
  supplier_prices_SUP-C_2026-08-28.csv       refuse
      value     unit_cost: 0 decimals, not 2: dollars are written like 2.10 in 15 rows, like 655 on line 2
```

In this milestone "refuse" is a word on the screen. The file is still loaded. Milestone 4 makes it real.

## 10. Side by side (`inspect.side_by_side`)

```sh
uv run python -m lotus_pipeline inspect --side-by-side
```

| Family of check | On | Tables |
|---|---|---|
| The key is unique | 5 of 7 | `sales_lines`, `till_totals`, `stores`, `stock_counts`, `members` |
| Values are allowed | 5 of 7 | `sales_lines`, `products`, `stores`, `stock_counts`, `members` |
| A row agrees with itself | 3 of 7 | `sales_lines`, `till_totals`, `products` |
| Cells are filled | 3 of 7 | `sales_lines`, `supplier_prices`, `members` |
| It did not change by surprise | 3 of 7 | `products`, `stores`, `supplier_prices` |
| It points at a row that exists | 3 of 7 | `sales_lines`, `stores`, `members` |
| Nothing is missing | 3 of 7 | `sales_lines`, `till_totals`, `stock_counts` |
| Two tables agree | 2 of 7 | `sales_lines`, `supplier_prices` |
| A row agrees with its neighbours | 1 of 7 | `products` |
| It has the columns we expect | 1 of 7 | `supplier_prices` |
| It looks like a normal day | 1 of 7 | `sales_lines` |
| Two counts of one thing agree | 1 of 7 | `sales_lines` |

Two families are on five of the seven cards. They are habits worth having on any table. Four families are on one card only. Each is there because of how that one table is made: only sales have a normal day and a second count, only a price list has neighbouring rows, only an outside file has columns that can move.

## 11. Tests

| Test file | What it proves |
|---|---|
| `test_other_checks.py` | **Every check has two kinds of test.** All 21 return nothing on four clean weeks. In the bonus week exactly six of them fire, on cases 10, 11, 12 and 13, with the planted rows. Every other fault is planted on a private copy, and beside it a check that stays silent. No member check returns personal data |
| `test_contract.py` | 13 files keep the contract and 2 break it. Each rule of a value, one by one. The contract's columns are the loader's columns |
| `test_sales_checks.py` | Milestone 2's sixteen checks, unchanged |
| `test_pipeline.py` and the others | The pipeline is unchanged: still five green checks and six wrong stores |
| `test_notebook_parity.py` | What the notebooks found by hand equals what the production checks find |
| `test_flow_page.py` | Every number on the flow page and the recap page comes from the code |

## 12. Where each part is built

| Notebook | Teaches | Design sections | Code |
|---|---|---|---|
| `01_a_price_has_a_start_and_an_end` | Validity dates | HLD 3, 4; LLD 4 | Checks 17 to 21 |
| `02_six_rows_wrong_everywhere` | | HLD 3; LLD 5 | Checks 22 to 25 |
| `03_a_gap_is_not_a_zero` | Snapshot table | HLD 4; LLD 6 | Checks 26 to 28 |
| `04_the_same_person_twice` | Duplicate, personal data | BRD 5; LLD 8 | Checks 34 to 37 |
| `05_a_file_you_do_not_control` | Schema, schema drift, breaking change | HLD 5; LLD 3, 7 | Checks 29, 30; `load.py` |
| `06_same_shape_new_meaning` | Change of meaning | HLD 6; LLD 7 | Checks 31, 32 |
| `07_a_contract_for_the_supplier` | Data contract | BRD 6; HLD 7; LLD 9 | `contract.py`, `contracts/` |
| `08_cards_side_by_side` | The same logic as a tested program | LLD 10, 11 | `inspect.py`, `tests/` |

## 13. What is still wrong

| What we have | What is missing |
|---|---|
| 37 checks on 7 tables, each with a reason and two kinds of test | A pipeline that runs them |
| A contract that says "refuse" | A file that is really refused |
| A list of bad rows | Somewhere for a bad row to go |

**The checks exist in notebooks. The pipeline does not run them, and a failure changes nothing.** Milestone 4 turns them into gates.
