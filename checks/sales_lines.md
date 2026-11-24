# Check card: `sales_lines`

**State after milestone 1:** questions 1 to 5 are answered. Questions 6 to 8 need the checks, and those are written in milestone 2.

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

Not yet. Milestone 2 writes the rule, the limit, the lens and how serious a failure is.

## 7. What happens when it fails?

Not yet. Milestone 4 decides: stop, set the rows aside, or warn. And who is told.

## 8. The proof

Not yet. Milestone 2 plants each fault and shows the check catching it, and one fault it does not catch.
