# Glossary

The working words of data quality, in the order this project teaches them.

A word enters this list in the milestone that teaches it, not before. Each entry has:

- the word, and its meaning in one or two short sentences
- the notebook where you meet it on Lotus's own rows
- a line you can say in an interview, with the example from this project

One shop word first, because the project uses it on every page. A **till** is the machine at the counter where a customer pays. It is also called a cash register. It is not a data word, so it has no row below.

For the full theory of each word, read the chapters of the [D8Loop Data Pipeline knowledge base](https://www.d8loop.com/modules/knowledge-corner/data-pipeline), in its section **Production, Quality & Advanced**.

## Milestone 1: Green checks, wrong number

| Word | Meaning | Taught in | Say it in an interview |
|---|---|---|---|
| **Data quality** | Data quality means the data is good enough to trust. | Milestone 1, notebook 1 | "Data quality is whether the people who read a table can trust it. At Lotus Retails the bonus was paid from a table that five green checks said was fine, and one store was $18.98 too high." |
| **Check** | A check is a query that looks for bad rows. No rows means the data passes. | Milestone 1, notebook 1 | "A check is a query that returns the rows that break a rule. Our uniqueness check grouped by the key and returned any key that was there twice." |
| **DAG** | The list of a pipeline's tasks and the order they run in. Airflow draws it as boxes and arrows. | Milestone 1, notebook 2 | "Our nightly DAG had five tasks in a line: the stores send files, load, clean, check, publish." |
| **Task** | One step of the pipeline that Airflow starts, such as `load`. | Milestone 1, notebook 2 | "Each task started a plain command, like `python -m lotus_pipeline load`. Airflow held no business logic, so I could run any step by hand." |
| **Dependency** | An arrow between two tasks. The second waits for the first, and does not run if the first fails. | Milestone 1, notebook 2 | "Publish depended on check. A failed check stopped the publish, so the pipeline was only as safe as what its checks could see." |
| **Silent failure** | Wrong data with no error. Every task succeeds, and a wrong number reaches the reader. | Milestone 1, notebook 3 | "A refund of $9.49 was stored as a positive amount, so it was added and not taken away. No task failed and five checks passed. That is a silent failure, and it is worse than a crash." |
| **Consumer** | Anything that reads a table: a person, a report or another table. | Milestone 1, notebook 4 | "Before I write a check I list the table's consumers. The sales lines had four groups: store managers, the head of retail operations, the replenishment team and finance." |
| **Impact** | Everything a wrong table touches: the tables built from it and the people who read them. Also called blast radius. | Milestone 1, notebook 4 | "I follow a table to its outputs and their readers to get its impact. A wrong stock count reached the order list, and through it the replenishment team and finance." |
| **Critical table** | A table where a wrong value costs someone money or a decision. It gets the most checks. | Milestone 1, notebook 4 | "I ranked seven tables by harm. Three were critical. One of them, the stores table, has six rows: every sale joins to it, so a wrong row there is wrong everywhere." |
| **Grain** | What one row of a table means. | Milestone 1, notebook 5 | "The grain of the sales table is one item line on one receipt. Store S03 had 113 rows and 34 receipts on one day, so counting rows as customers is three times too high." |
| **Profiling** | Reading a table before you check it: count the rows, find the smallest and largest values, count the empty cells. | Milestone 1, notebook 6 | "I profile a table before writing its checks. Split by line type, the largest refund amount was plus $9.49. A refund should never be above zero, and one query found it." |
| **The six dimensions** | Six questions for any table: is it complete, unique, valid, accurate, consistent and fresh? | Milestone 1, notebook 7 | "I sort checks under six questions: complete, unique, valid, accurate, consistent, fresh. Our five standard checks covered three. Accuracy needs a second source, so we used the tills' own totals." |

The six dimensions have formal names at work: completeness, uniqueness, validity, accuracy, consistency and timeliness.

## Milestone 2: Checks with a reason

| Word | Meaning | Taught in | Say it in an interview |
|---|---|---|---|
| **Not-null check** | A check that a cell is filled. The skill is saying in which rows an empty cell is wrong. | Milestone 2, notebook 1 | "A not-null check on the key was green while an empty amount hid behind it, because a sum ignores an empty cell. I check the cells that must be filled, and I say in which rows: a refund must name its receipt, a sale must not." |
| **Unique check** | A check that one thing is stored once. | Milestone 2, notebook 2 | "Our unique check was on a line id that the export made new every time, so it could never find a receipt sent twice. I moved it to the receipt and its line number, and it found 171 rows." |
| **Composite key** | A key made of more than one column, such as `receipt_id` + `line_no`. | Milestone 2, notebook 2 | "The true key of a sales line was a composite key: the receipt and the position on it. One column alone did not name a row in the real world." |
| **Range check** | A check that a number is between a smallest and a largest believable value. | Milestone 2, notebook 3 | "A scale sent grams as kilograms: 1,180 kg of bananas on one line, $1,522. I set a range per unit, 20 kg at most for weighed goods. A limit on money would have fired on a real sale of 400 bottles." |
| **Format check** | A check that a text value has the right shape, such as a receipt id. | Milestone 2, notebook 3 | "A receipt id had to be the line's own store, its own till and six digits. A format check costs one line and catches an id that can never be joined." |
| **Business rule** | A rule of how the company works, such as "a refund gives money back". The data cannot tell you the rule. A person must. | Milestone 2, notebook 4 | "Five data checks passed a refund stored as a positive amount. Only a business rule could see it: a refund gives money back. I got the rules from finance and store operations, not from the table." |
| **Reference data** | A small list that people keep by hand, and that other data is judged against: prices, opening hours, holidays. | Milestone 2, notebook 4 | "The price check joined each sale to the price list valid that day, and found one store charging yesterday's price on 12 packs of butter. Reference data must be checked too, because every row is judged against it." |
| **Validity and accuracy** | A valid value is allowed by the rules. An accurate value matches the real world. A value can be valid and wrong. | Milestone 2, notebook 4 | "The butter lines were valid: a real product, a positive price, a store that was open. They were not accurate, because the price was a day old. Validity can be tested on the row. Accuracy needs something outside it." |
| **Reconciliation** | Comparing your numbers with another source that saw the same thing. | Milestone 2, notebook 5 | "Each till adds up its own day. I reconciled the sales lines against those totals, per till per day, to the cent. That one check found seven of the nine planted faults." |
| **Control total** | A total counted a second way, kept only to be compared. The till's own total is one. | Milestone 2, notebook 5 | "Nobody read the till totals table, and every money check leaned on it. It was a control total. I gave it three checks of its own, because a control you cannot trust is worse than none." |
| **Referential integrity** | Every row that points to another table finds a row there. | Milestone 2, notebook 5 | "The cleaning step joined sales to products. A product nobody had typed yet meant three sales lines were dropped, with no error. I check referential integrity on the raw rows, before the join can hide them." |
| **Orphan row** | A row that points to something that does not exist, such as a sale for a product nobody typed. | Milestone 2, notebook 5 | "Three sales of pumpkin soup were orphan rows: the product was not in the price list yet. An inner join deletes orphans silently, and the store's sales were $17.97 too low." |
| **Baseline** | The number you compare with. It is what normal looks like. | Milestone 2, notebook 6 | "My baseline for a store's day was the same weekday in the four weeks before. One number for every day would not work: a Saturday had twice the lines of a Tuesday." |
| **Limit** | The line between good and bad in a check. Also called a threshold. | Milestone 2, notebook 6 | "I chose the limit from the data. In four clean weeks a store stayed between 74% and 131% of its normal day, so I set 60% to 140%. A file sent twice came in at 226%." |
| **Anomaly** | A value far from the baseline. It is a reason to look, not a proof of a fault. | Milestone 2, notebook 6 | "A day at 226% of normal is an anomaly. It does not say what is wrong. The uniqueness check and the till total said it: the whole file was there twice." |
| **False positive** | An alarm when nothing is wrong. Also called a false alarm. | Milestone 2, notebook 7 | "My volume check fired on a public holiday in all six stores. Six false positives in one night teach people to ignore the check, so I gave it a calendar." |
| **False negative** | A fault with no alarm. | Milestone 2, notebook 7 | "Leaving holidays out of the volume check has a price: a real fault on a holiday is a false negative for that check. I wrote that down, and the till total still covers the day." |
| **Seasonality** | A pattern that repeats with the calendar: Saturdays are busy, holidays are quiet. | Milestone 2, notebook 7 | "Sales had a weekly seasonality, so I compared a Friday with earlier Fridays. A simple average works for a weekly pattern. A holiday that moves each year needs a calendar, or a model." |

## Milestone 3: Every table is different

| Word | Meaning | Taught in | Say it in an interview |
|---|---|---|---|
| **Validity dates** | The first and the last day a row is true, such as the days a price applies. | Milestone 3, notebook 1 | "A price row had validity dates, so the key was the product and its start date. I checked each row against its neighbours: no two prices on one day, and no day without one. A gap of two days silently dropped every sale of that product." |
| **Snapshot table** | A table that holds the state of something on each day, such as a stock count. A missing day is a gap, not a zero. | Milestone 3, notebook 3 | "The stock counts were a snapshot table. One missing count was read as an empty shelf, and the store ordered 18 bottles where 15 were needed. To find a missing row I first built the rows I expected." |
| **Duplicate** | One real thing stored more than once under different keys, such as one person with two member ids. | Milestone 3, notebook 4 | "One customer signed up twice, with capital letters in her email the second time. The member id was unique and the email as typed was unique. I cleaned the email first, then grouped by it, and found her." |
| **Personal data** | Data about a person: a name, an email, a phone number. It must not travel into logs and check results. | Milestone 3, notebook 4 | "A check prints its bad rows, and those lines end up in logs and in quarantine tables. So my checks on members return the member id only, and a test fails if a name or an email appears in any result." |
| **Schema** | The shape of a table or a file: the column names, the type of each column, and the rules for the values. | Milestone 3, notebook 5 | "A loaded table cannot tell you which columns the file had. I kept the first line of every file in its own table, and compared it with the schema we expected." |
| **Schema drift** | A schema that changes without anyone agreeing: a column renamed, added or removed. | Milestone 3, notebook 5 | "A supplier renamed its cost column. The load ran, twelve products had no cost, and nothing failed. That is schema drift, and only a check on the file's own columns sees it on the night it arrives." |
| **Breaking change** | A change that removes or changes something a reader uses, so old readers fail. Its opposite is an additive change, which only adds. | Milestone 3, notebook 5 | "A renamed column is one breaking change and one additive change: the old name is gone, and a new one appeared. I refuse a file for the first and only report the second." |
| **Change of meaning** | The shape stays the same and the meaning changes, such as dollars sent as cents. No schema check can see it. | Milestone 3, notebook 6 | "A supplier sent cents where dollars were agreed. Every column was there and every value was a valid number, 100 times too high. A value can only be judged beside something else, so I compared it with the last file and with the shelf price." |
| **Data contract** | A written agreement between the producer of data and its consumers: an owner, the columns, the rules for the values, and a change policy. | Milestone 3, notebook 7 | "We wrote a one-page contract for the suppliers' file and held every file against it at the door. It refused 2 of 15 files before they were loaded. A check on a table speaks after the load. A contract speaks before it." |

## Milestones 4 and 5

Not built yet. Their words are added when each milestone is built.
