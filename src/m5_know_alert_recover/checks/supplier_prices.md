# Check card: `supplier_prices`

**State after milestone 5:** all eight questions are answered, and the card is complete.

## 1. What does one row mean?

One product's cost from one supplier, starting on one day. The key is `supplier_id` + `product_id` + `price_date`.

Three suppliers each send one file a week.

## 2. Who uses this table, and what do they decide with it?

| Who | What they decide | What goes wrong for them when the data is bad |
|---|---|---|
| Finance | What to pay each supplier | A wrong cost is wrong money leaving the company |
| Finance and the replenishment team, through the cost on `replenishment_list` | Whether to approve tomorrow's order | An order with no cost. Or an order that looks 100 times too dear |

## 3. How is the data made? So what can go wrong?

**Lotus does not make this data, and does not control it.** Another company's program writes the file. That company can change the program on any day, and nobody at Lotus is told.

| Step | Made by | What can go wrong |
|---|---|---|
| The supplier's system writes a CSV file | A program Lotus has never seen | A column is renamed, moved or removed. A column is added |
| The cost is written | The same program | The unit of the number changes: cents where dollars were agreed |
| The file is sent once a week | The supplier | The same file again. No file |

## 4. Which of those would hurt most?

| Rank | Worry | Why it is here |
|---|---|---|
| 1 | The same shape, a new meaning: cents for dollars | Every value is a valid number, and every one is 100 times too high. Planted case 11 |
| 2 | A renamed column | The load runs and the cost is empty. Planted case 10 |
| 3 | A cost per kilogram for a product sold by the piece | A wrong payment, on a valid row |
| 4 | A new column | It hurts nobody today. It is worth knowing: the supplier changed something |

## 5. What must be prepared before it can be checked?

| Worry | What must be prepared first |
|---|---|
| A renamed column | Keep the first line of every file. The loaded table cannot say which columns the file had |
| Cents for dollars | Join each cost to the same product in the supplier's last file. And to the shelf price |
| A wrong unit | Join to `products` |

Two of these look at the file **before** it becomes a table. That is new. And the written agreement they are held against is in [`contracts/supplier_prices.yml`](../contracts/supplier_prices.yml).

## 6. The check

Five checks. Since milestone 4 each check is a dbt test, in `dbt_project/tests/`.

| # | Check | The limit | Lens | Question | How serious |
|---|---|---|---|---|---|
| 29 | A file has the columns we expect | No expected column is missing. A column we did not expect is reported too | Data | Valid? | High when a column is missing. Low when a column is new |
| 30 | Every supplier row has a cost | No empty unit_cost, product, supplier, unit or date | Data | Complete? | High |
| 31 | A cost is near the cost in the last file | A cost is at most 2 times the last one, and at least half of it | Business | Accurate? | High |
| 32 | A product costs less than it sells for | unit_cost_cents is below unit_price_cents | Business | Accurate? | High |
| 33 | A supplier row fits its product | Each supplier + product + date is there once. The product exists. The unit is the product's unit | Data | Consistent? | High |

Check 29 looks at every file of every table, not only the suppliers'. It is on this card because this is the file most likely to change.

## 7. What happens when it fails?

Each check is now a dbt test, and a gate in the pipeline acts on it. The test's own file says what a failure does: `dbt_project/tests/<gate>/<check>.sql`.

| # | Check | Gate | When it fails |
|---|---|---|---|
| 29 | A file has the columns we expect | `gate_landing` | **Refuse the file.** None of its rows go on. The last good file stays in use |
| 30 | Every supplier row has a cost | `gate_clean` | **Quarantine.** The bad rows are set aside with this reason. The good rows carry on |
| 31 | A cost is near the cost in the last file | `gate_clean` | **Quarantine.** The bad rows are set aside with this reason. The good rows carry on |
| 32 | A product costs less than it sells for | `gate_clean` | **Quarantine.** The bad rows are set aside with this reason. The good rows carry on |
| 33 | A supplier row fits its product | `gate_clean` | **Quarantine.** The bad rows are set aside with this reason. The good rows carry on |

**Who is told?** Since milestone 5, Airflow tells a person on the night, from a task callback. A **page** wakes someone. A **ticket** is for today. A **message** waits in a digest. Each alert is sent once, says what to do, and links to `RUNBOOK.md`.

| # | Check | Level | Who is told |
|---|---|---|---|
| 29 | A file has the columns we expect | Ticket: the file was refused | Finance and the buying team, for a supplier's file. The store, for its own |
| 30 | Every supplier row has a cost | Ticket | Finance, and the buying team |
| 31 | A cost is near the cost in the last file | Ticket | Finance, and the buying team |
| 32 | A product costs less than it sells for | Ticket | Finance, and the buying team |
| 33 | A supplier row fits its product | Ticket | Finance, and the buying team |

## 8. The proof

| Planted fault | Caught by | A check that does not catch it, and why |
|---|---|---|
| Case 10: `SUP-B` renames `unit_cost` to `cost_per_unit`, 08-25. Twelve products have no cost | 29, on the night the file arrives. And 30 | 31: an empty cost cannot be compared with anything |
| Case 11: `SUP-C` sends cents, 08-28. Fifteen costs are 100 times too high | 31 and 32 | 29: every column is there. 30: every cost is filled. The shape is perfect |

The contract catches both at the door, before the load: `uv run python -m lotus_pipeline contract` refuses 2 of the 15 files.

A cost that really changes is not a fault. Three costs change by a few cents during the five weeks, and no check returns them.
