# Check card: `sales_lines`

**State after milestone 4:** all eight questions are answered. One part of question 7 is still open: who is told. That is milestone 5.

A check card is filled in **before** any check is written. It is one short page. It says why a check exists.

## 1. What does one row mean?

One item line on one receipt. A receipt with three products has three rows.

The true key of a row is `receipt_id` + `line_no`. The column `line_id` is not a true key: the export program makes a new one every time it writes a file.

## 2. Who uses this table, and what do they decide with it?

| Who | What they decide | What goes wrong for them when the data is bad |
|---|---|---|
| Store managers, through `daily_store_sales` | Nothing. They are **paid** from it: 1% of the week's net sales | Wrong pay. On 2026-08-24 store `S03` was $18.98 too high |
| Head of retail operations | She signs off the bonus every Monday | She signs a wrong number, and cannot say which one |
| The replenishment team, through `replenishment_list` | How much of each product to order for tomorrow | Too high wastes food. Too low leaves empty shelves |

Two outputs and three groups of people read this table. It is the most critical table Lotus has.

## 3. How is the data made? So what can go wrong?

A **till** is the machine at the counter where a customer pays. It is also called a cash register.

| Step | Made by | What can go wrong |
|---|---|---|
| A cashier scans a product | A person and a till | A wrong quantity. A weight in the wrong unit |
| The till stamps the time | The till's clock | A wrong clock, so a wrong day |
| The till takes the price | The price list in the till | Yesterday's price |
| A customer brings something back | A person and a till | A refund with the wrong sign |
| The export program writes the night's file | A program in the store's back office | A line twice. A receipt twice. A whole file twice |
| The store sends the file | A back-office computer | No file. A late file |

## 4. Which of those would hurt most?

Ranked by the money a person loses, and by how long it stays hidden.

| Rank | Worry | Why it is here |
|---|---|---|
| 1 | A store's whole day is missing or doubled | The largest error in pay: a full day of sales |
| 2 | Money with the wrong sign or the wrong size | One line can be larger than a day. Seen in notebook 3 |
| 3 | The same sale counted twice | Small each time, and it never shows as an error |
| 4 | A sale on the wrong day | It can fall out of the bonus week |
| 5 | A line the join drops | Sales are too low, and no row is left to look at |
| 6 | An empty `member_id` | It hurts nobody. A customer may pay without a loyalty card. **No check** |

Not every worry gets a check. Number 6 has no answer to question 2, so it gets none.

## 5. What must be prepared before it can be checked?

| Worry | What must be prepared first |
|---|---|
| A whole day missing or doubled | Count the rows per store per day, and compare with a normal day for that store |
| Wrong sign or wrong size | Add the lines up per till per day, and hold the total against `till_totals` |
| The same sale twice | Group by `receipt_id` + `line_no`, not by `line_id` |
| A sale on the wrong day | Join to `stores` for the opening hours |
| A line the join drops | Compare the raw rows with the clean rows |

Four of the five need a join, a total or a comparison first. None of them is a one-line check.

## 6. The check

Thirteen checks. Each is a query that returns the bad rows. Since milestone 4 each check is a dbt test, in `dbt_project/tests/`.

"How serious" is a first opinion. Milestone 4 turns it into what the pipeline does.

| # | Check | The limit | Lens | Question | How serious |
|---|---|---|---|---|---|
| 1 | A cell that must be filled is filled | No empty amount, quantity, price, time, product or receipt. A refund names its receipt | Data | Complete? | High |
| 2 | One item line is stored once | Each `receipt_id` + `line_no` is there once | Data | Unique? | High |
| 3 | A receipt id has the right shape | The line's own store, its own till, six digits | Data | Valid? | Low |
| 4 | A line adds up | `amount_cents` is `quantity` × `unit_price_cents` | Data | Consistent? | High |
| 5 | Every sale is for a product that exists | No raw line is without a product or a store | Data | Consistent? | High |
| 6 | A refund gives money back | A refund is below zero. A sale is above zero | Business | Valid? | High |
| 7 | A refund is not larger than the sale | The receipt exists, and gave at least that much | Business | Consistent? | Medium |
| 8 | The price charged is the price valid that day | Equal to the price list, exactly | Business | Accurate? | Medium. The table is right. The store lost money |
| 9 | Nothing is sold while the store is closed | `sold_at` is inside the opening hours of that day | Business | Valid? | High |
| 10 | A weight is a believable weight | Not above 20 kg on one line. Pieces are whole numbers | Business | Valid? | High |
| 11 | The receipts add up to the till's own total | Equal to the cent, per till per day | Business | Accurate? | High |
| 12 | Every open store reported | At least one line per open store per day | Operational | Complete? | High |
| 13 | A store's day is near its normal day | 60% to 140% of the same weekday in the four weeks before | Operational | Complete? | Medium. It is a reason to look |

Worry 6 of question 4, an empty `member_id`, still has no check. It has no answer to question 2.

## 7. What happens when it fails?

Each check is now a dbt test, and a gate in the pipeline acts on it. The test's own file says what a failure does: `dbt_project/tests/<gate>/<check>.sql`.

| # | Check | Gate | When it fails |
|---|---|---|---|
| 1 | A cell that must be filled is filled | `gate_clean` | **Quarantine.** The bad rows are set aside with this reason. The good rows carry on |
| 2 | One item line is stored once | `gate_clean` | **Quarantine** the extra copies. The first copy carries on, so the day is right again |
| 3 | A receipt id has the right shape | `gate_clean` | **Quarantine.** The bad rows are set aside with this reason. The good rows carry on |
| 4 | A line adds up | `gate_clean` | **Quarantine.** The bad rows are set aside with this reason. The good rows carry on |
| 5 | Every sale is for a product that exists | `gate_clean` | **Quarantine.** The bad rows are set aside with this reason. The good rows carry on |
| 6 | A refund gives money back | `gate_clean` | **Quarantine.** The bad rows are set aside with this reason. The good rows carry on |
| 7 | A refund is not larger than the sale | `gate_clean` | **Quarantine.** The bad rows are set aside with this reason. The good rows carry on |
| 8 | The price charged is the price valid that day | `gate_clean` | **Warn.** The run continues, and the warning is recorded |
| 9 | Nothing is sold while the store is closed | `gate_clean` | **Quarantine.** The bad rows are set aside with this reason. The good rows carry on |
| 10 | A weight is a believable weight | `gate_clean` | **Quarantine.** The bad rows are set aside with this reason. The good rows carry on |
| 11 | The receipts add up to the till's own total | `gate_publish` | **Block**, when a difference has no explanation. When a missing file or a row set aside explains it: the store's day is marked "do not pay yet", with the reason |
| 12 | Every open store reported | `gate_publish` | **Warn**, and a partial load. The other stores are published. This store's day is marked "do not pay yet: no sales file arrived" |
| 13 | A store's day is near its normal day | `gate_clean` | **Warn.** The run continues, and the warning is recorded |

**Who is told?** Not yet. A gate can fail at 02:00 and nobody knows. Milestone 5 sends the alert.

## 8. The proof

Each fault is planted on a known day. The check must return exactly its rows. Every check is also run on four clean weeks, where it must return nothing.

| Planted fault | Caught by | A check that does not catch it, and why |
|---|---|---|
| 1. A refund stored as a positive amount, `S03` 08-24 | 6, 4 and 11 | 7: the size of the refund is right. Only its sign is wrong |
| 2. One receipt twice, `S01` 08-25 | 2 and 11 | Standard check 3: every `line_id` is new |
| 3. A weight in grams, `S05` 08-27 | 10 and 11 | 4: the export worked the amount out again, so the line adds up |
| 4. Yesterday's price, `S04` 08-28 | 8 | 11: the till charged the same wrong price, so both counts agree |
| 5. A sale at 03:10, `S02` 08-30 | 9 and 11 | 13: one receipt does not change the size of a day |
| 6. A product nobody typed, `S06` 08-29 | 5 and 11 | Every check on the clean table: the rows are gone |
| 7. No file, `S04` 08-25 | 12 and 11 | 13: a day with no rows has nothing to compare |
| 8. A late file, `S06` 08-27 | 12, on that night only | Every check, one night later: the file has arrived |
| 9. The whole file twice, `S02` 08-28 | 2, 13 and 11 | 12: the store did report |

Three things look wrong and are not. No check returns them.

| Looks wrong | Why no check fires |
|---|---|
| A public holiday, 08-26, about 40% of a normal day | The calendar says it is a holiday. Check 13 leaves calendar days out |
| A closed store, `S01` 08-27 | The calendar says it is closed. Check 12 does not expect it |
| A sale of 400 bottles, `S02` 08-29 | Check 10 limits weight, not pieces. 400 pieces is a whole number |

The cost of the calendar: a real fault on a holiday is not caught by check 13. That is a false negative, taken on purpose.
