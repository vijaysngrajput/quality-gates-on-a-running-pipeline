# Check card: `products`

**State after milestone 3:** questions 1 to 6 and 8 are answered. Question 7 needs gates, and those are built in milestone 4.

## 1. What does one row mean?

One product at one price. A product whose price changed once has two rows: the old price with its last day, and the new price with its first day.

The key is `product_id` + `valid_from`. `product_id` alone is **not** the key.

## 2. Who uses this table, and what do they decide with it?

| Who | What they decide | What goes wrong for them when the data is bad |
|---|---|---|
| Every sales line, through the join in the cleaning step | Nothing. It is joined to the price valid on its day | No price on that day: the line is dropped. Two prices on that day: the line is counted twice |
| Store managers and the head of retail operations, through `daily_store_sales` | Pay, and the sign-off | Sales too low or too high, for every store at once |
| The replenishment team | Which products to order | A retired product is ordered for ever |
| The price check on `sales_lines` | Whether a store charged today's price | A wrong price list makes every right sale look wrong |

It is a critical table. It has about 40 rows, and every sale in the company passes through it.

## 3. How is the data made? So what can go wrong?

The buying team types it. A person, a form, and no till.

| Step | Made by | What can go wrong |
|---|---|---|
| A new product is typed | A person | It is typed late. The tills already sell it |
| A price changes | A person closes the old row and opens a new one | The old row is not closed: two prices on one day. The new row starts too late: a day with no price |
| A price is typed | A person | A slip of the finger: 5190 where 519 was meant |
| A product is retired | A person | The status changes and the last day is forgotten |

## 4. Which of those would hurt most?

| Rank | Worry | Why it is here |
|---|---|---|
| 1 | Two prices on one day | Every sale of that product is counted twice, in every store. The standard unique check then fails on `sales_lines`, where nothing is wrong: it points at the wrong table |
| 2 | A day with no price | Every sale of that product is dropped, in every store |
| 3 | A price typed wrong | Every till charges it. Customers notice before the data team does |
| 4 | A product that is not typed yet | Its sales are dropped. This is planted case 6, and check 5 on `sales_lines` catches it |
| 5 | A retired product with no last day | It stays on the order list |
| 6 | An untidy product name | It hurts nobody. **No check** |

## 5. What must be prepared before it can be checked?

| Worry | What must be prepared first |
|---|---|
| Two prices on one day | Join the table to itself: each price row against the later rows of the same product |
| A day with no price | For each price that ends, look for the one that starts the next day |
| A price typed wrong | Join each price to the price before it |

A table of prices is checked against **itself**, row against neighbouring row. Nothing like that was needed for `sales_lines`.

## 6. The check

Five checks. The SQL is in `lotus_pipeline/other_checks.py`.

| # | Check | The limit | Lens | Question | How serious |
|---|---|---|---|---|---|
| 17 | A product has one price on any day | No two price rows of one product cover the same day | Data | Unique? | High |
| 18 | A product on sale has a price on every day | An active price that ends is followed, the next day, by another price | Data | Complete? | High |
| 19 | A product row makes sense | A name. A price above zero. A unit of each or kg. A status of active or retired. A start that is not after its end | Data | Valid? | High |
| 20 | A new price is near the old price | A new price is at most 2 times the old one, and at least half of it | Business | Accurate? | Medium. A real price can double. A person confirms |
| 21 | A retired product has a last day | status is retired exactly when the last price row has an end date and nothing follows | Business | Consistent? | Low |

## 7. What happens when it fails?

Not yet. Today a failed check changes nothing: the checks are run by hand, with `python -m lotus_pipeline inspect`. Milestone 4 decides for each one: stop, set the rows aside, or warn. And who is told.

## 8. The proof

The price list is right on every day of the five weeks. Each fault is planted in a test, on a private copy.

| Fault planted in a test | Caught by | A check that does not catch it, and why |
|---|---|---|
| A second open price row for Whole milk | 17 | 18: there is no day without a price. There is a day with two |
| The new Eggs price starts two days late | 18 | 17: no two rows overlap |
| A price of zero, a unit of "box" | 19 | 20: there is no earlier price to compare with |
| Eggs at 5190, not 519 | 20 | 19: 5190 is a perfectly allowed price |
| Bagels retired, with no last day | 21 | 18: the row never ends, so no gap follows it |
