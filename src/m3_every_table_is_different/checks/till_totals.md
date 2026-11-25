# Check card: `till_totals`

**State after milestone 3:** questions 1 to 6 and 8 are answered. Question 7 needs gates, and those are built in milestone 4.

A till is the machine at the counter where a customer pays. It is also called a cash register.

## 1. What does one row mean?

One till's own total for one day. Two tills in a store make two rows a day.

The key is `store_id` + `till_id` + `business_date`.

## 2. Who uses this table, and what do they decide with it?

| Who | What they decide | What goes wrong for them when the data is bad |
|---|---|---|
| Nobody reads it as an output | | |
| Check 11 on `sales_lines` reads it | Whether a day's money is right | A wrong total here makes a right day look wrong, or a wrong day look right |

This table has no reader of its own. It still gets a card, because it is a **control total**: a total counted a second way, kept only to be compared. Seven of the nine planted sales faults are caught by it.

## 3. How is the data made? So what can go wrong?

| Step | Made by | What can go wrong |
|---|---|---|
| The till adds up each receipt as it prints it | The till | Very little. It is the same machine that took the money |
| The cashier closes the till at night | A person | A till is not closed, so it sends no total |
| The store sends the file | A back-office computer | No file. The file twice |

It never passes through the export program that writes `sales_lines`. That is what makes it a second, independent count.

## 4. Which of those would hurt most?

| Rank | Worry | Why it is here |
|---|---|---|
| 1 | A till sends no total | The money check has nothing to compare with |
| 2 | A total stored twice | The join would double it, and every day would look wrong |
| 3 | A total that does not add up | The control itself cannot be trusted |
| 4 | A total that is late by minutes | It hurts nobody. **No check** |

## 5. What must be prepared before it can be checked?

| Worry | What must be prepared first |
|---|---|
| A till sends no total | The list of tills a day should have: two for every open store |
| A total stored twice | Group by the key |
| A total that does not add up | Nothing. The three numbers are on one row |

## 6. The check

Three checks. The SQL is in `lotus_pipeline/till_checks.py`.

| # | Check | The limit | Lens | Question | How serious |
|---|---|---|---|---|---|
| 14 | A till has one total for a day | Each `store_id` + `till_id` + `business_date` is there once | Data | Unique? | High |
| 15 | A till's total adds up | Net is gross less refunds. No number is below zero | Business | Consistent? | High |
| 16 | Every till of an open store sent its total | Two totals per open store per day | Operational | Complete? | High |

## 7. What happens when it fails?

Not yet. Milestone 4 decides.

## 8. The proof

The tills' own totals are right on every day of the five weeks, so no planted case breaks this table. Each fault is planted in a test, on a private copy.

| Fault planted in a test | Caught by | A check that does not catch it, and why |
|---|---|---|
| One total stored twice | 14 | 15: both copies add up |
| Net is not gross less refunds | 15 | 14: the row is there once |
| One till sends no total | 16, and check 11 on `sales_lines` | 15: a row that is not there cannot fail |
