# Check card: `stores`

**State after milestone 3:** questions 1 to 6 and 8 are answered. Question 7 needs gates, and those are built in milestone 4.

A till is the machine at the counter where a customer pays. It is also called a cash register.

## 1. What does one row mean?

One store, with its opening hours and its manager. Six rows. The key is `store_id`.

## 2. Who uses this table, and what do they decide with it?

| Who | What they decide | What goes wrong for them when the data is bad |
|---|---|---|
| Every sales line, through the join in the cleaning step | Nothing. It is joined to its store | A store that is missing: all its sales are dropped. A store that is there twice: all its sales are doubled |
| Store managers | Their name is on the bonus report | The bonus goes to the wrong person |
| The opening-hours check on `sales_lines` | Whether a sale was made while the store was closed | Wrong hours make right sales look wrong |

Six rows, and a critical table. **Wrong here is wrong everywhere.**

## 3. How is the data made? So what can go wrong?

It was typed once, by head office, and it is rarely changed.

| Step | Made by | What can go wrong |
|---|---|---|
| A store is typed, once | A person | A wrong hour, a wrong name |
| A store is changed | A person, perhaps once a year | Nobody tells the data team. A row is deleted or added by mistake |
| The whole table is sent every night | A program | Almost nothing |

## 4. Which of those would hurt most?

| Rank | Worry | Why it is here |
|---|---|---|
| 1 | A store is missing, or there twice | A whole store's sales vanish or double |
| 2 | A file names a store that is not in the table | The join drops every row of that store |
| 3 | A row changed and nobody said so | The table should stand still. A change is either news or a mistake |
| 4 | Hours that make no sense | The opening-hours check reads them |

## 5. What must be prepared before it can be checked?

| Worry | What must be prepared first |
|---|---|
| A store is missing, or there twice | Nothing but a count. For a table this small, the exact number of rows is a fact you can know |
| A file names an unknown store | Collect the `store_id` of every store file, and join to `stores` |
| A row changed | Keep yesterday's file, and compare it with today's, row by row |

On `sales_lines` a row count told you nothing: "not empty" passed with one store missing. Here the row count is the best check there is. **The same check is useless on one table and exact on another.**

## 6. The check

Four checks. The SQL is in `lotus_pipeline/other_checks.py`.

| # | Check | The limit | Lens | Question | How serious |
|---|---|---|---|---|---|
| 22 | There are six stores, each once | Exactly 6 rows, and no store_id twice | Data | Unique? | High |
| 23 | A store row makes sense | A name, a town and a manager. It opens before it closes. It has at least one till | Data | Valid? | High |
| 24 | Every file is from a store we know | Every store_id in the three store files is in the stores table | Data | Consistent? | High |
| 25 | The stores table is the same as yesterday | No row is new, gone or different. A real change is confirmed by a person | Operational | Consistent? | Low. It is a reason to ask a person |

## 7. What happens when it fails?

Not yet. Today a failed check changes nothing: the checks are run by hand, with `python -m lotus_pipeline inspect`. Milestone 4 decides for each one: stop, set the rows aside, or warn. And who is told.

## 8. The proof

The stores table is right, and the same, on every day of the five weeks. Each fault is planted in a test, on a private copy.

| Fault planted in a test | Caught by | A check that does not catch it, and why |
|---|---|---|
| Store `S06` is deleted | 22 | 23: the five rows that are left all make sense |
| Store `S01` is there twice, so there are six rows again | 22 | A plain row count: six is the right number |
| A till total from store `S07` | 24 | 22: the stores table itself is fine |
| `S02` now closes at 23:00 | 25 | 23: 23:00 is a perfectly allowed hour |
| A store that closes when it opens | 23 | 25, the next day: the table is the same as yesterday |
