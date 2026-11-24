# Proposal: the tables, the planted cases, and the answer key

**Status:** approved by the owner on 2026-11-24, with every recommendation in section 8 accepted. Built in `store_systems/` and in milestone 1.

This was the first stop that `Quality_Gates_on_a_Running_Pipeline.md` asks for. It proposed three things:

1. The columns of the seven tables, and the two outputs.
2. The thirteen planted cases, each with its date, its store and its amount.
3. The answer key, worked by hand.

Some details changed while building, and the code is the truth for each:

- **A normal day** is $422 to $1,405 of net sales for a store, not $700 to $1,500. The world has 7,330 receipts and 24,326 lines.
- **The amounts that waited for the generator** are now known. Case 7: `S04`'s missing Tuesday is $587.42. Case 9: `S02`'s doubled Friday adds $1,034.28. Case 8: `S06`'s late Thursday is $616.44, missing for one night. The full key is in `answer_key/`.
- **Case 12** needed a busy day to be worth showing. Three hand-chosen receipts sell 8 bottles of milk at `S05` on 08-28. The store sold 9 and has 3 left. The right order is 15, and with no count it is 18.
- **Standard check 5** runs on the clean sales lines, added up per store per day. It does not read `daily_store_sales`, because the checks run before publish.
- **`store_systems fix`** takes `--case 1` or `--all-faults`, not a day.
- **Under Docker the landing folder is a Docker volume**, not the project folder. So Linux needs no file-permission set-up. On your own computer it is `data/landing`.
- **The example ids** in section 2 were invented before the product list existed. Olive oil is `P-024`, butter is `P-003`.

Section 8 lists the decisions that were made. Section 9 lists where the project document, the knowledge base and reality disagree.

---

## 1. The calendar

| Thing | Proposal |
|---|---|
| Business days | 35 days: Monday **2026-07-27** to Sunday **2026-08-30**. Five full weeks |
| Weeks 1 to 4 | 07-27 to 08-23. Clean. They are the "normal days" a check compares with |
| Week 5, the bonus week | Monday 08-24 to Sunday 08-30. Every planted case is in this week |
| The Monday question | Asked on Monday **2026-08-31**, about week 5 |
| The nightly run | 02:00, for the business day that just ended. Store files are due by 01:00 |
| Stores | Six, in three fictional towns: Ashby (`S01`, `S02`), Calder (`S03`, `S04`), Fenwick (`S05`, `S06`) |
| Tills | Two per store: `T1`, `T2` |
| Clock | One local clock for all stores. No UTC, no time zone trap. That lesson belongs to Daily Orders |
| Money | Whole cents, US dollars. Prices include tax |
| Volume | 21 to 61 receipts per store on a normal day, 1 to 6 lines each. About 24,000 lines in all |
| A store's normal day | Net sales between about $420 and $1,400 |
| Seed | One fixed seed. Every learner gets the same rows |

Why five weeks: a check "against a normal day" needs four earlier Tuesdays to compare a Tuesday with.

Why small stores: a learner must be able to add up one store's day and believe the result.

## 2. The seven tables, as the stores send them

Every table arrives as CSV files in a landing folder. The loader adds three columns to every raw table: `_file_name`, `_received_at`, `_loaded_at`.

### 2.1 `sales_lines`: one item line on one receipt

One file per store per night: `sales_lines_S03_2026-08-24.csv`.

| Column | Type | Example | Rule |
|---|---|---|---|
| `line_id` | text | `S03-0824-000117` | Given by the export program, new on every export. The key the starter pipeline trusts |
| `receipt_id` | text | `S03-T1-004217` | Store, till, and the till's own running number |
| `line_no` | integer | `2` | Position on the receipt, from 1. `receipt_id` + `line_no` is the true key of a line |
| `store_id` | text | `S03` | |
| `till_id` | text | `T1` | |
| `sold_at` | timestamp | `2026-08-24 17:42:10` | The till's clock. Local time |
| `product_id` | text | `P-024` | |
| `quantity` | decimal, 3 places | `2.000`, `1.180` | Pieces, or kilograms for goods sold by weight. Negative on a refund |
| `unit_price_cents` | integer | `949` | Price for one piece or one kilogram |
| `amount_cents` | integer | `1898` | `quantity × unit_price_cents`, rounded to a cent. Negative on a refund |
| `line_type` | text | `sale` | `sale` or `refund` |
| `refund_of_receipt_id` | text, may be empty | `S03-T2-004101` | Only on a refund: the receipt being refunded |
| `member_id` | text, may be empty | `M-0117` | The loyalty member, if the card was scanned |

There is no "business date" column. The starter pipeline takes the day from `sold_at`. Case 5 depends on that.

`line_id` is the important design choice. It is new on every export, so a receipt or a file that is sent twice still has unique `line_id`s. That is how cases 2 and 9 pass the standard "key is unique" check.

### 2.2 `till_totals`: one till's own total for one day

One file per store per night: `till_totals_S03_2026-08-24.csv`. Two rows.

| Column | Type | Example | Rule |
|---|---|---|---|
| `business_date` | date | `2026-08-24` | The day the till was open |
| `store_id`, `till_id` | text | `S03`, `T1` | The key, with `business_date` |
| `receipts` | integer | `19` | Receipts printed, refunds included |
| `gross_sales_cents` | integer | `61240` | Money taken for sales |
| `refunds_cents` | integer | `949` | Money given back. A positive number |
| `net_total_cents` | integer | `60291` | `gross_sales_cents − refunds_cents` |
| `closed_at` | timestamp | `2026-08-24 22:06:31` | When the till was closed |

The till adds this up itself, inside the till. It never passes through the export program. So it is a second, independent count.

### 2.3 `products`: one product at one price

One full file every night: `products_2026-08-24.csv`. About 45 rows for 40 products.

| Column | Type | Example | Rule |
|---|---|---|---|
| `product_id` | text | `P-003` | With `valid_from`, the key |
| `product_name` | text | `Butter 250g` | |
| `category` | text | `Dairy` | One of eight categories |
| `unit` | text | `each` | `each` or `kg` |
| `unit_price_cents` | integer | `349` | The price while this row is valid |
| `valid_from` | date | `2026-07-27` | First day of this price |
| `valid_to` | date, may be empty | `2026-08-27` | Last day of this price. Empty means "still valid" |
| `status` | text | `active` | `active` or `retired` |

A product with one price change has two rows. Two price changes are in the data: one clean (Eggs, from 08-10) and one that goes wrong at a store (Butter, case 4).

### 2.4 `stores`: one store

One full file every night: `stores_2026-08-24.csv`. Six rows.

| Column | Type | Example |
|---|---|---|
| `store_id` | text | `S02` |
| `store_name` | text | `Lotus Ashby Market Square` |
| `town` | text | `Ashby` |
| `opens_at`, `closes_at` | time | `08:00`, `22:00` |
| `manager_name` | text | `Dana Okafor` |
| `tills` | integer | `2` |

### 2.5 `stock_counts`: one product in one store on one day

One file per store per night: `stock_counts_S05_2026-08-24.csv`. 40 rows.

| Column | Type | Example | Rule |
|---|---|---|---|
| `count_date` | date | `2026-08-24` | With `store_id` and `product_id`, the key |
| `store_id`, `product_id` | text | `S05`, `P-006` | |
| `counted_qty` | decimal, 3 places | `22.000` | Pieces or kilograms on the shelf at closing time |
| `counted_by` | text | `scanner` | `scanner` or `hand` |
| `counted_at` | timestamp | `2026-08-24 21:15:00` | |

### 2.6 `supplier_prices`: one product's cost from one supplier

Three suppliers, one file a week each: `supplier_prices_SUP-B_2026-08-25.csv`. `SUP-A` sends on Mondays, `SUP-B` on Tuesdays, `SUP-C` on Fridays.

| Column | Type | Example | Rule |
|---|---|---|---|
| `supplier_id` | text | `SUP-B` | |
| `product_id` | text | `P-012` | With `supplier_id` and `price_date`, the key |
| `unit_cost` | decimal, dollars | `2.10` | **Dollars with two decimals.** Not cents. Case 11 depends on that |
| `unit` | text | `each` | `each` or `kg` |
| `price_date` | date | `2026-08-25` | The day the cost starts |

Each product has one supplier.

### 2.7 `members`: one loyalty member

One full file every night: `members_2026-08-24.csv`. About 300 rows, and a few new ones each day.

| Column | Type | Example | Rule |
|---|---|---|---|
| `member_id` | text | `M-0117` | The key the app gives |
| `full_name` | text | `Priya Nair` | Personal data |
| `email` | text | `priya.nair@example.com` | Personal data. Typed by the customer |
| `phone` | text, may be empty | | Personal data. Optional in the app, so empty is normal |
| `home_store_id` | text | `S03` | |
| `joined_on` | date | `2026-07-30` | |
| `marketing_opt_in` | boolean | `true` | May we email them |

Every name is invented, and every email ends in `@example.com`.

### 2.8 The two outputs

`daily_store_sales`: one row per store per day.

| Column | Rule in the starter pipeline |
|---|---|
| `sales_date` | The date of `sold_at` |
| `store_id` | |
| `receipts` | Count of different `receipt_id` |
| `lines` | Count of rows |
| `net_sales_cents` | Sum of `amount_cents` |

`replenishment_list`: one row per store per product, for tomorrow.

| Column | Rule in the starter pipeline |
|---|---|
| `for_date` | The day after the run's business day |
| `store_id`, `product_id` | |
| `sold_yesterday` | Sum of `quantity` for that store, product and day |
| `stock_counted` | `counted_qty` of that day. **Zero when there is no row** |
| `order_qty` | `2 × sold_yesterday − stock_counted`, never below zero |
| `unit_cost_cents` | The supplier's newest `unit_cost`, turned into cents |
| `order_cost_cents` | `order_qty × unit_cost_cents` |

The bonus report reads `daily_store_sales`. The bonus is **1% of the store's net sales for the week**, so an error in sales is an error in pay that a learner can work out.

`replenishment_list` uses `supplier_prices`, so cases 10 and 11 reach an output. `members` and `till_totals` reach no output. Marketing reads the clean `members` table itself, and nobody reads `till_totals`. That is the point of `till_totals`.

### 2.9 The five standard checks

All five run on the clean tables, after cleaning and before publish.

| # | Check | On |
|---|---|---|
| 1 | The table is not empty | `sales_lines`, for the run's day |
| 2 | The key is not null | `sales_lines.line_id` |
| 3 | The key is unique | `sales_lines.line_id` |
| 4 | The status is an allowed value | `sales_lines.line_type` is `sale` or `refund` |
| 5 | The total is positive | Each store's day of `sales_lines` adds up to more than zero |

Check 5 is on the store's day, not on a line. A refund line is negative by design, so a line check would fail on good data.

## 3. The source system

One shared folder at the root, `store_systems/`. It is the outside world, like `order_system/` in Daily Orders. It is not a server. It is a small program that writes files.

| Command | What it does |
|---|---|
| `python -m store_systems deliver --day 2026-08-24` | Writes the files the stores sent for that day into the landing folder |
| `python -m store_systems deliver --through 2026-08-23` | The same for every day up to that one: the four clean weeks |
| `python -m store_systems fault <kind> --store S02 --day 2026-08-28` | Makes it misbehave: `late-file`, `file-twice`, `rename-column`, `bad-prices`, `no-file` |
| `python -m store_systems fix --case 1` | The source sends it right the next time. Used in milestone 5's recovery |
| `python -m store_systems reset` | An empty landing folder, and no faults |

Inside, one list is the truth: the **till journal**. It holds every receipt as it really happened. The files are written from it. The thirteen planted cases are differences between the journal and the files. The answer key is worked out from the journal alone.

## 4. The planted cases

All in week 5. Each amount below is chosen by hand, so it can be checked with a pencil.

Prices used: Olive oil 500ml 949. Whole milk 2L 289. Sourdough loaf 525. Cheddar 200g 449. Tomatoes 399 a kg. Bananas 129 a kg. Coffee beans 250g 899. Oat milk 1L 349. Eggs, 12: 519. Dark chocolate 100g 279. Butter 250g 349, then 399 from 08-28. Still water 500ml 59.

| # | Case | Day, store | What is in the files | Starter pipeline's error |
|---|---|---|---|---|
| 1 | A refund stored as a positive amount | Mon 08-24, `S03` | A customer returns 1 Olive oil. The line has `line_type = refund`, `quantity = -1`, and `amount_cents = +949`. It should be −949 | Net sales **+$18.98** |
| 2 | One receipt twice, with different line ids | Tue 08-25, `S01` | Receipt of 2 Whole milk (578), 1 Sourdough (525), 1 Cheddar (449), 0.640 kg Tomatoes (255). Total 1,807. It is in the file twice | Net sales **+$18.07** |
| 3 | A weight in grams, not kilograms | Thu 08-27, `S05` | 1.180 kg of Bananas is written as `quantity = 1180`. The amount becomes 1180 × 129 = 152,220, not 152 | Net sales **+$1,520.68** |
| 4 | A price changed yesterday, charged at the old price | Fri 08-28, `S04` | Butter is 399 from 08-28. `S04` sells 12 at 349 | Net sales are what the till took. The store is **$6.00 short** |
| 5 | A sale at 03:10 in a store that closes at 22:00 | Sun 08-30, `S02` | The till restarts after a power cut and its clock is wrong. A receipt sold at 21:40 is stamped `2026-08-31 03:10`. Coffee beans (899), 2 Oat milk (698), Eggs (519), Dark chocolate (279). Total 2,395 | The sale lands on Monday 08-31. `S02`'s week is **−$23.95** |
| 6 | A sale for a product that is not in `products` | Sat 08-29, `S06` | 3 receipts each have 1 Pumpkin soup, `P-041`, at 599. The buying team has not typed it yet | The join drops the lines. Net sales **−$17.97** |
| 7 | One store sends no file | Tue 08-25, `S04` | No `sales_lines` file. The other five stores send theirs | `S04` has no row for that day. The whole day is missing |
| 8 | One store's file arrives a day late | Thu 08-27, `S06` | The file arrives at 00:40 on 08-29, not 08-28 | The day is empty after the 08-27 run, and right after the 08-28 run |
| 9 | One store's whole file sent twice | Fri 08-28, `S02` | A second copy of the file, with new `line_id`s | The whole day is doubled |
| 10 | The supplier renames a column | Tue 08-25, `SUP-B` | `unit_cost` is now called `cost_per_unit` | `unit_cost_cents` is empty for `SUP-B`'s products |
| 11 | The supplier sends cost in cents | Fri 08-28, `SUP-C` | `2.10` is now `210` | Every cost of `SUP-C` is 100 times too high |
| 12 | A stock count is missing for one day | Fri 08-28, `S05` | No row for Whole milk 2L | Stock is read as zero. The store over-orders by its real stock |
| 13 | The same member twice | Wed 08-26 | `M-0117` is `priya.nair@example.com`. New member `M-0304` is `Priya.Nair@Example.com` | Two emails, two sets of points |

Where each fault is born matters, and the learner is shown it:

- Cases 1, 2, 3 and 9 are born in the **export program**. The till's own total is right, so `till_totals` also catches them.
- Cases 4 and 5 are born **in the till**. The till's total agrees with the lines. Only a business rule catches them.

Monday 08-24 has exactly one case, the refund. That is the day of milestone 1 and of the two-minute run.

### Things that look wrong and are not

| What | Day, store | Why it is right |
|---|---|---|
| A public holiday with low sales | Wed 08-26, all stores. "Founders' Day", an invented holiday | About 40% of a normal day. The stores open for shorter hours |
| A store closed for one day | Thu 08-27, `S01` | A planned refit. No file, and that is correct |
| A real sale of 400 items | Sat 08-29, `S02` | A café buys 400 Still water at 59. One line of 23,600 |

No check may fire on these three.

## 5. The answer key

### 5.1 The rule

True net sales of a store on a day: add every sale and take away every refund, for the receipts the till really printed on that business day. It is worked out from the till journal, by code that shares nothing with the pipeline.

### 5.2 What the starter pipeline gets wrong, by store, for week 5

| Store | Cases | Week's error | Bonus error at 1% |
|---|---|---|---|
| `S01` | 2 | +$18.07 | +$0.18 |
| `S02` | 5, 9 | +$1,034.28 and −$23.95: +$1,010.33 | +$10.10 |
| `S03` | 1 | +$18.98 | +$0.19 |
| `S04` | 7 | −$587.42 | −$5.87 |
| `S05` | 3 | +$1,520.68 | +$15.21 |
| `S06` | 6 | −$17.97. Case 8 has healed by Monday | −$0.18 |

**All six stores are wrong. All five checks are green on all seven nights.** That is the project in one line.

### 5.3 What the key cannot hold yet

The full totals of each store and day depend on the ordinary generated receipts. They do not exist until the generator does, and I will not invent them. The generator is the first code I write. It saves the full key to a file. A test then checks that the key equals the journal, and that every row of 5.2 holds.

The quantities of case 12 are also fixed then, and frozen by the same test.

### 5.4 The final answer, on Monday 08-31

The project document says: yes for five stores, no for one. My proposal for the one: **`S04`**. Its Tuesday file never arrives before sign-off. The other faults can be set aside or repaired. A day with no data cannot. The row that says why is in the run history: `S04`, 08-25, no file received.

On the two-minute run for Monday 08-24, the store marked "do not pay yet" is `S03`, with the refund line set aside.

## 6. Names

| Thing | Proposal |
|---|---|
| Milestone folders | `m1_green_checks_wrong_number`, `m2_checks_with_a_reason`, `m3_every_table_is_different`, `m4_gates_and_quarantine`, `m5_know_alert_recover` |
| Pipeline package | `lotus_pipeline` |
| Source system | `store_systems/` at the root |
| Warehouse layers | Schemas `raw`, `clean` and `output` |
| Docker names | Containers `lotus-warehouse` and `lotus-airflow`. Image `lotus-airflow:3.4.0` |
| Ports | Warehouse 5450. Airflow 8080, changeable with one setting. Daily Orders uses 5440, so both projects can run together |
| Repository | `quality-gates-on-a-running-pipeline` |

## 7. Checked against today's reality

- Apache Airflow **3.4.0** is still the newest image on Docker Hub. Daily Orders uses it, so a learner who did that project has already downloaded it.
- I will check the callback, backfill and dbt "store failures" documentation before the milestones that use them, as the project document asks.

## 8. Decisions, all accepted

| # | Question | My recommendation |
|---|---|---|
| 1 | The first two minutes must work **before Docker**, and the warehouse is **Postgres**, which needs Docker. Which gives way? | The two-minute run uses DuckDB, a database in one file. Everything under Docker uses Postgres. The pipeline's SQL is written to run on both, and a test proves it. Daily Orders also used DuckDB first and Postgres second |
| 2 | Is the calendar right: five weeks, every planted case in week 5, an invented holiday? | Yes |
| 3 | Case 4 does not make net sales wrong. The till really took that money. Is that acceptable? | Yes. It is a warning to the store, not an error in the table. It shows that a check can protect money that no output shows |
| 4 | A closed store sends no file. A store with a broken computer also sends no file. Only a calendar tells them apart | Add one small reference file in milestone 2: `calendar.csv`, with the holiday and the closure. It is not an eighth pipeline table, and I say so |
| 5 | Is the bonus 1% of weekly net sales? | Yes. It turns every error into an amount of pay |
| 6 | Is `S04` the store that cannot be paid at the end? | Yes |
| 7 | The names in section 6 | Yes |

## 9. Where the project document, the knowledge base and reality differ

I read the three chapters. The project document asks for their words.

| The project document says | The chapters say | My recommendation |
|---|---|---|
| The six dimensions: completeness, uniqueness, validity, accuracy, consistency, timeliness | Six **questions**: Complete? Unique? Valid? Accurate? Consistent? Fresh? The word "dimension" is only on the index card | Teach the six questions. Give the six dimension names once, as the words used at work. Say "fresh" is also called timeliness |
| Silent failure, in the quality chapter | Defined in the schema chapter: "a wrong number reaches the dashboard" | Use that definition. Link to the schema chapter for it |
| Consumer, blast radius, critical table, in the observability chapter | "Consumer" is defined in the schema chapter. "Blast radius" is only on the index card. "Critical table" is not there | Teach "consumer" with the chapter's words. Define "impact" and "critical table" in this project, and say "blast radius" is another name |
| Profiling, grain | Not in the three chapters. "Grain" is in the Daily Orders glossary | Use the Daily Orders meaning of grain. Define profiling in this project |
| Threshold | "Limit", and "band" for a range | Write "limit". Say "also called a threshold" once |
| The five pillars of observability | Five **signals**: freshness, volume, schema, distribution, lineage | Teach five signals in milestone 5. Say "also called pillars" once |
| Warn or block | Warn, quarantine or block | The same three. No conflict |
| Milestone 1's chapter for DAG and task | The Airflow knowledge base, chapters 3 and 4 | Link to those two |

Two more things, outside the chapters:

- **Milestone 1 installs Docker and Airflow.** The project document also says the first run must not need them. Both hold if the README's two-minute run comes first, and milestone 1's own setup comes second. I will write it that way.
- **The gallery card** says "Planned" and "Intermediate". That is for the website. I will remind you when the repository is ready.
