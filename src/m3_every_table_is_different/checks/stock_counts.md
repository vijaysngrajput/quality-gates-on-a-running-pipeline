# Check card: `stock_counts`

**State after milestone 3:** questions 1 to 6 and 8 are answered. Question 7 needs gates, and those are built in milestone 4.

## 1. What does one row mean?

One product in one store on one day: how much was on the shelf at closing time.

The key is `count_date` + `store_id` + `product_id`.

This table is a **snapshot**. It does not hold things that happened, like a sale. It holds the state of something on each day. So a day that is missing is a **gap**. It is not a zero.

## 2. Who uses this table, and what do they decide with it?

| Who | What they decide | What goes wrong for them when the data is bad |
|---|---|---|
| The replenishment team, through `replenishment_list` | How much to order for tomorrow: twice what sold, less what is on the shelf | A missing count is read as an empty shelf, and they order too much. Food is wasted |
| Finance, through the cost on the order list | What the order will cost | They approve an order that is too large |

## 3. How is the data made? So what can go wrong?

A person walks the store before closing. Packed goods are scanned. Goods sold by weight are counted by hand.

| Step | Made by | What can go wrong |
|---|---|---|
| A shelf is scanned | A person and a scanner | A shelf is skipped |
| Loose goods are weighed and typed | A person | A slip of the finger. A count that is not possible |
| The store sends the file | A back-office computer | A product twice |

## 4. Which of those would hurt most?

| Rank | Worry | Why it is here |
|---|---|---|
| 1 | A product is not counted | The pipeline reads the gap as zero and over-orders. Planted case 12 |
| 2 | A product is counted twice | Two numbers for one shelf. The join doubles the row |
| 3 | A count that cannot be true | Below zero, or half a bottle |
| 4 | A count that is a little off | People miscount. It hurts little, and nothing can test it. **No check** |

## 5. What must be prepared before it can be checked?

| Worry | What must be prepared first |
|---|---|
| A product is not counted | The list of counts a day **should** have: every open store, every product on sale. Then look for what is not there |
| A product is counted twice | Group by the key |
| A count that cannot be true | Join to `products` for the unit |

A check can only return rows that exist. To find a row that is **missing**, you must first build the rows you expected. That is the lesson of a snapshot.

## 6. The check

Three checks. The SQL is in `lotus_pipeline/other_checks.py`.

| # | Check | The limit | Lens | Question | How serious |
|---|---|---|---|---|---|
| 26 | A snapshot has a row for every day | Every product on sale has a count, in every open store, on every day | Data | Complete? | High |
| 27 | A product is counted once a day | Each count_date + store_id + product_id is there exactly once | Data | Unique? | High |
| 28 | A count is a believable count | Not empty, not below zero. Pieces are whole numbers. Counted on its own day, by scanner or by hand | Data | Valid? | Medium |

## 7. What happens when it fails?

Not yet. Today a failed check changes nothing: the checks are run by hand, with `python -m lotus_pipeline inspect`. Milestone 4 decides for each one: stop, set the rows aside, or warn. And who is told.

## 8. The proof

| Fault | Caught by | A check that does not catch it, and why |
|---|---|---|
| Planted case 12: no count for Whole milk 2L at `S05` on 08-28. The store sold 9 and has 3. The right order is 15. The pipeline orders 18 | 26 | 28: a row that is not there cannot have a wrong value. And all five standard checks |
| A product counted twice, planted in a test | 27 | 26: the count is there |
| A count of −3, and of 2.5 bottles, planted in a test | 28 | 27: the row is there once |

A closed store is not a gap. `S01` was closed on 08-27 for a planned refit, the calendar says so, and check 26 does not expect its counts.
