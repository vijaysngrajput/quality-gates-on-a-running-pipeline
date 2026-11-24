# Glossary

The working words of data quality, in the order this project teaches them.

A word enters this list in the milestone that teaches it, not before. Each entry has:

- the word, and its meaning in one or two short sentences
- the notebook where you meet it on Lotus's own rows
- a line you can say in an interview, with the example from this project

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

## Milestones 2 to 5

Not built yet. Their words are added when each milestone is built.
