# Quality Gates on a Running Pipeline

## Project overview

**Quality Gates on a Running Pipeline** is a learning project about one skill: deciding which quality checks a table needs, and why.

It is made for the **Data quality and reliability** category of the D8Loop Data Engineering Projects gallery: https://www.d8loop.com/modules/knowledge-corner/data-engineering-projects#data-quality-and-reliability

The gallery describes that category in one line: the pipeline already runs, and now you make it safe when the data is wrong or late. It promises that a project in it lets the learner practise five things. This project must deliver all five:

- Quality checks
- Quarantine
- Alerts
- Backfills
- Run history

Its card in the gallery reads: "Add checks, alerts and a safe backfill to a pipeline, then break it on purpose and recover."

### The idea behind the project

Read this part twice. Everything else serves it.

Most engineers write the same few checks on every table: the row count is not zero, the key is not null, the key is unique, the total is positive. They write them because a downstream team asked, or because a template had them. They cannot say what each check protects. So the checks pass, and the data is still wrong.

This project teaches the step that comes **before** writing a check:

1. **Every table is different.** A table of sales, a table of products and a file from a supplier fail in different ways. So they need different checks.
2. **A check has a reason.** The learner must be able to say who uses the table, what they decide with it, and what goes wrong for them when the data is bad.
3. **Many checks need work first.** A useful check is often not one line. The data must be prepared: reshaped, added up, joined to another table, or compared with a normal day. Only then can it be tested against a rule.
4. **A check looks through one of three lenses.** A **business** check tests a rule of how the company works. A **data** check tests the values themselves. An **operational** check tests the pipeline: did it run, on time, with a normal amount of data.

The learner still writes the common checks. They learn them properly: what each one catches, and what it cannot catch.

### Who it is for

It is a **starter** project. The reader knows some Python and a little SQL. They have never built a pipeline, and never used dbt, Docker or Airflow. They do not need to have done another D8Loop project first.

Because the reader is a beginner, **the pipeline is given to them ready-made**, already running on Apache Airflow. They run it and read it in milestone 1. They do not build it. Their hours go into quality.

The pipeline runs on Airflow because that is what most data teams use to run their checks: a check is a task, and a failed check stops the tasks after it. Airflow needs Docker. The reader has never installed either, so **the setup is written click by click** for macOS, Linux and Windows (see "Tools").

It is the third project of the D8Loop Data Engineering Projects series. The first two are [Retail Profit Stack](https://github.com/vijaysngrajput/retail-profit-stack) and [Daily Orders: API to Warehouse](https://github.com/vijaysngrajput/daily-orders-api-to-warehouse). This project follows the same standard, described under "How every milestone is built" below. Read both READMEs and one milestone folder of Daily Orders before you start, so the three projects look like one series. Its page on D8Loop will follow this one: https://www.d8loop.com/modules/knowledge-corner/data-engineering-projects/daily-orders-api-to-warehouse

There is **no AI in this project**. It needs no API key and costs nothing to run. Everything runs on the learner's laptop.

## The business

**Lotus Retails** is a fictional chain of supermarkets. It has six stores in three towns. Customers buy at the tills in the store. All data is fictional and generated.

Keep it different from the two earlier projects: Retail Profit Stack is an online outdoor shop, and Daily Orders is an online grocery with delivery. Lotus Retails is **physical stores with tills**. That gives it problems the others do not have: a till that sends its file late, goods sold by weight, a stock count done by hand, a price file from a supplier.

Four groups at Lotus Retails use the data. Each is hurt in a different way when it is wrong. This is the heart of the project, so make each one real:

- **Store managers** are paid a weekly bonus on their store's net sales. A wrong number is wrong pay.
- **The replenishment team** orders tomorrow's stock from yesterday's sales. Too high wastes food. Too low leaves empty shelves.
- **Finance** pays suppliers from the price file and the goods received. A wrong price is wrong money leaving the company.
- **Marketing** emails loyalty members. One person stored twice gets two emails, and a wrong address reaches a stranger.

### The tables

Seven tables, different on purpose. Propose the exact columns before milestone 1 and stop for approval.

| Table | What one row is | How it is made | Why its checks differ |
|---|---|---|---|
| `sales_lines` | One item line on one receipt | Each till writes it, each store sends a file every night | Money. Refunds. Goods sold by weight. A store's file can be late, missing or sent twice |
| `till_totals` | One till's own total for one day | The till prints it when it is closed | A second, independent count of the same money. It exists to be compared with `sales_lines` |
| `products` | One product, with its price and the dates that price is valid | Typed by the buying team | It changes. A price has a start and an end. A product can be retired |
| `stores` | One store, with its opening hours | Typed once, rarely changed | Tiny. Wrong here is wrong everywhere |
| `stock_counts` | One product in one store on one day | Counted by hand or by scanner | A snapshot. A missing day is a gap, not a zero |
| `supplier_prices` | One product's cost from one supplier | A CSV file the supplier sends | Lotus does not control it. Columns move, units change, files repeat |
| `members` | One loyalty member | Typed by the customer in an app | People. The same person twice, half-filled rows, personal data |

`till_totals` is small, and it is the simplest way to teach reconciliation: two counts of the same money that must agree.

The pipeline that is given to the learner turns these into two outputs:

- `daily_store_sales`: one row per store per day, with net sales. The store managers' bonus is paid from it.
- `replenishment_list`: one row per store per product, with how much to order.

## The business question

Every Monday the head of retail operations signs off the store managers' bonus. She asks:

> Can I pay the bonus from last week's net sales, store by store? And if I cannot trust a number, which one, and why?

The second sentence is the project. Today nobody can answer it. The pipeline has checks, and they are all green.

## What the learner is given, and what they build

**Given, ready-made:**

```
Store files  →  raw tables  →  clean tables  →  daily_store_sales, replenishment_list
(the source)    (as received)   (typed, joined)   (what people read)
        \______ one Airflow DAG, every night, with five standard checks, all green ______/
```

1. **A small source system** that writes each day's files for each store. It has a clock the learner controls, so they can move to the next day. It can be told to misbehave: send a file late, send it twice, change a column, put a bad value in a row.
2. **The starter pipeline**: load, clean, build the two outputs. Short, plain Python and SQL the learner can read in one sitting.
3. **One Airflow DAG** that runs the starter pipeline every night: a task to load, a task to clean, a task for the checks, a task to publish. The learner opens Airflow's page, sees the tasks, and runs a day by hand.
4. **Five standard checks** that the DAG already runs: the table is not empty, the key is not null, the key is unique, the status is an allowed value, the total is positive.
5. **An answer key**: the true numbers, worked out from the source's own record with no pipeline code, as in Daily Orders.

**Built by the learner, one milestone at a time:**

1. A **check card** for each table: the written reasoning that comes before any check.
2. **Checks** through all three lenses, each written by hand first, then as a dbt test.
3. **Gates**: each group of checks becomes an Airflow task. A failed gate stops the tasks after it, sets rows aside, or only warns.
4. **Quarantine**: bad rows set aside with a reason, good rows carry on.
5. **Run history, freshness and volume checks**, and **alerts** sent by Airflow to the right person.
6. A **recovery**: fix the source, re-run only the affected day with an Airflow backfill, and write down what happened.

**Who does what.** Keep this clear on every page, because beginners confuse the two tools:

- **dbt holds the rule.** "The receipts must equal the till total" is a dbt test: a query that returns the rows that break the rule.
- **Airflow runs the rule and acts on the result.** It decides when the check runs, what waits for it, what happens when it fails, who is told, and it remembers every run.

## The check card

This is the method the learner repeats for every table. It is the most important thing they take away. Each card is one short page, kept in the repository as `checks/<table>.md`, and filled in **before** any check is written.

1. **What does one row mean?** The grain, in one sentence.
2. **Who uses this table, and what do they decide with it?**
3. **How is the data made?** By a till, by a person typing, by an outside company. So what can go wrong?
4. **Which of those would hurt most?** Rank them. Not every problem deserves a check.
5. **What must be prepared before it can be checked?** A join, a total, a comparison with another table or with a normal day.
6. **The check.** The rule, the limit, the lens (business, data or operational), and how serious a failure is.
7. **What happens when it fails?** Stop, set the rows aside, or warn. And who is told.
8. **The proof.** Plant the fault and show the check catching it. Show one fault it does not catch.

A check that has no answer to question 2 is deleted. Say this plainly to the learner.

## Milestones

**Five milestones. No more.** A long list of milestones looks like a long road, and a learner who sees a long road stops. Each milestone below holds several ideas, so the shortness has to come from the notebooks: **one idea per notebook, about ten minutes each, six to eight notebooks in a milestone.** If a notebook grows past that, split the notebook, not the milestone.

Each milestone ends with something that is **still wrong**. That is the reason the next one exists. State it plainly at the end of each milestone.

1. **Green checks, wrong number.**
   Install Docker, start Airflow, and run the given pipeline for one night. All five standard checks pass. Then work out one store's net sales by hand from its receipts. The two numbers differ. Find the cause: a refund stored as a positive amount.
   Then ask the question nobody asked: who uses each table, and what happens to them when it is wrong? Rank the tables by harm. Meet the check card. Read `sales_lines` before checking it: rows per store per day, the smallest and largest values, the empty cells. Each surprise becomes a candidate check.
   *Still wrong: we found one error by luck, and we have a list of worries. We have no checks that would have caught it.*

2. **Checks with a reason.**
   Write the checks for `sales_lines`, through each lens, by hand first.
   **Data checks**: empty, duplicate, out of range, wrong format. The lesson is in the detail: unique on which columns, empty in which rows. Show the common version missing a planted fault, then the careful version catching it.
   **Business checks**: a refund is not larger than the sale, the price charged is the price valid that day, nothing is sold while the store is closed, a weight is a believable weight. Each needs the data prepared first, usually by a join.
   **Checks across two tables**: the receipts must add up to the till's own total, and every sale is for a product that exists.
   **Checks against a normal day**: how much does this store usually sell on a Tuesday? Choose the limit, and meet the false alarm: a holiday is not an error.
   *Still wrong: one table is done well. There are five more, and they are nothing like it.*

3. **Every table is different.**
   Fill in a check card and write the checks for `products`, `stores`, `stock_counts`, `supplier_prices` and `members`. Each brings its own problem: a price with a start and an end date, a missing day that is read as zero stock, the same person stored twice, a supplier who renames a column or sends the cost in cents.
   Put the cards side by side: which checks appear on every card, which on only one, and why. Write a small contract for the supplier's file.
   *Still wrong: the checks exist in notebooks. The pipeline does not run them, and a failure changes nothing.*

4. **Gates and quarantine.**
   Move every check into dbt as a test, with a level of seriousness. Then put the checks into the Airflow DAG as gate tasks: one when the files land, one after cleaning, one before the outputs are published. Run the same check in each place and see what it protects.
   Decide for each check what a failure does. **Block**: the publish task does not run, and the output keeps its last good numbers. **Quarantine**: the bad rows are set aside with their reason, the good rows carry on, and the output says what it is missing. **Warn**: the run continues and the warning is recorded.
   Then test the tests: plant each fault and prove its gate catches it.
   *Still wrong: a gate fails at 02:00, and nobody knows until a store manager complains.*

5. **Know, alert, recover.**
   **Know**: is the pipeline healthy? Did it run, did it finish on time, is the data fresh, did every store report? Read Airflow's run history. Write down what "on time" means as a number.
   **Alert**: Airflow tells the person who can act: what failed, how bad it is, what to do. Then send too many alerts and feel the cost. Group them, rank them, silence what does not need a person.
   **Recover**: four drills, one at a time. A store's file arrives twice. A column is renamed. A file is a day late. A batch of impossible prices. For each: which gate caught it, what was set aside, what the manager saw. Then fix the source, re-run only that day with an Airflow backfill, and prove the numbers equal the answer key. Write a short incident note.
   *Nothing is still wrong. Say which further work belongs to other projects.*

At the end the learner answers the Monday question: yes for five stores, no for one, and here is the row that says why.

## The planted cases

Hand-chosen, on known dates, so every one can be checked with a pencil. Each must pass all five standard checks of the starter pipeline. That is the point. Propose the full list with dates and amounts before milestone 1, and stop for approval.

| Case | What happens | Caught by | Lens |
|---|---|---|---|
| A refund stored as a positive amount | Net sales are too high for one store | Refund rule, and the till total | Business |
| One receipt in the file twice, with different line ids | The key is still unique. Sales are counted twice | Unique on the right columns, and the till total | Data |
| A weight entered in grams, not kilograms | One line of bananas costs more than the store's whole day | Believable range per product | Business |
| A price that was changed yesterday, charged at the old price | Each line is valid. The money is wrong | Price valid on that date | Business |
| A sale at 03:10 in a store that closes at 22:00 | A till with a wrong clock, or a wrong day | Store hours | Business |
| A sale for a product that is not in `products` | The join drops the line silently, and sales are too low | Every sale has a product | Data |
| One store sends no file | Every check passes: there are no rows to fail | Volume against a normal day, and "every store reported" | Operational |
| One store's file arrives a day late | Yesterday's number is low, and changes tomorrow | Freshness, and run history | Operational |
| One store's whole file sent twice | Sales for that store double | Volume against a normal day, and the till total | Operational |
| The supplier renames a column | The load runs. The cost column is empty | Expected columns | Data |
| The supplier sends cost in cents | Every cost is 100 times too high, and still a valid number | Change against the last file | Business |
| A stock count is missing for one day | The gap is read as zero stock, and the store over-orders | A snapshot has a row for every day | Data |
| The same member twice, with different letter case in the email | Two emails, two sets of points | Uniqueness after cleaning | Data |

For each case the learner sees three things: the standard checks passing, the harm to a named person, and the check that catches it.

Also keep a short list of **things that look wrong and are not**: a public holiday with low sales, a store closed for one day, a real sale of 400 items. The learner must not write a check that fires on these. A check that cries wolf gets switched off.

## What the learner learns

The words come from the D8Loop **Data Pipeline** knowledge base, from its section **Production, Quality & Advanced**: https://www.d8loop.com/modules/knowledge-corner/data-pipeline

Open that page first. Read these three chapters before you design the milestone that teaches them, and use their words and definitions, so the project and the knowledge base agree:

- Data Quality & Validation: `data-quality-and-validation`
- Data Observability & Monitoring: `data-observability-and-monitoring`
- Schema Evolution & Data Contracts: `schema-evolution-and-data-contracts`

The chapters are under `https://www.d8loop.com/modules/knowledge-corner/data-pipeline/`. These supporting chapters are in other knowledge bases, and the learner is linked to them where they fit:

- SQL: `sql/data-quality-debugging-auditing`
- Python: `python/validation-contracts-error-handling`
- dbt: `dbt/sources-freshness-and-staging-models`, `dbt/dbt-tests-generic-singular-and-custom`, `dbt/unit-tests-in-dbt`, `dbt/model-contracts-versions-and-access`
- Data Pipeline: `data-pipeline/incremental-processing-and-backfills`

**The project is the practice. The knowledge base is the theory.** Do not re-teach a chapter. Give the idea in two or three sentences on Lotus's rows, and link to the chapter.

### How a word is taught

The same five rules as Daily Orders. They matter more than the list.

1. **Pain first, name second.** The learner sees a store manager about to be overpaid before they hear "reconciliation".
2. **One or two new words per notebook.** Never a list of definitions.
3. **Each word is shown on Lotus's rows.** The input rows, the code, the result.
4. **Each word is compared with its opposite.** Warn with block. Stop with quarantine. A fixed limit with a normal-day limit. A check that fires with one that stays silent on the same fault.
5. **Each word ends as an interview line.** One or two sentences the learner can say, with the example from this project in it.

Keep a `GLOSSARY.md` at the root, as Daily Orders does: the word, its meaning in one or two short sentences, the milestone and notebook that teach it, and the interview line. A word enters the glossary in the milestone that teaches it, not before.

### Where each word is taught

| Milestone | Words the learner must be able to explain afterwards | Chapter |
|---|---|---|
| 1 | Data quality. Check. Silent failure: wrong data with no error | `data-quality-and-validation` |
| 1 | Consumer. Impact, also called blast radius. Critical table | `data-observability-and-monitoring` |
| 1 | Profiling. Grain. The six dimensions: completeness, uniqueness, validity, accuracy, consistency, timeliness | `data-quality-and-validation` |
| 1 | DAG, task and dependency, only as far as needed to read the given pipeline | `airflow/` knowledge base, first chapters |
| 2 | Not-null, unique, range and format checks. Composite key. A check is a query that returns the bad rows | `data-quality-and-validation`, `sql/data-quality-debugging-auditing` |
| 2 | Business rule. Validity against accuracy. Reference data | `data-quality-and-validation` |
| 2 | Reconciliation. Control total. Referential integrity. Orphan row | `sql/data-quality-debugging-auditing` |
| 2 | Baseline. Threshold. Anomaly. False positive and false negative. Seasonality | `data-observability-and-monitoring` |
| 3 | Snapshot table. Validity dates on a price. Duplicate people. Personal data | `data-quality-and-validation` |
| 3 | Schema. Schema drift. Breaking change. Data contract. A change of meaning with the same shape | `schema-evolution-and-data-contracts` |
| 4 | Generic test, singular test, severity, stored failures. A unit test against a data test | `dbt/dbt-tests-generic-singular-and-custom`, `dbt/unit-tests-in-dbt` |
| 4 | Gate. Gate placement. Warn or block. Write, audit, publish. An upstream failure stops the tasks after it | `data-quality-and-validation` |
| 4 | Quarantine. Rejected rows. Dead-letter table. Partial load | `data-quality-and-validation`, `python/validation-contracts-error-handling` |
| 5 | Freshness. Volume. Run history. SLI, SLO and SLA. Observability and its five pillars | `data-observability-and-monitoring`, `dbt/sources-freshness-and-staging-models` |
| 5 | Alert. Callback. Routing. Alert fatigue. Runbook | `data-observability-and-monitoring`, `airflow/monitoring-logging-alerting-and-production-failures` |
| 5 | Incident. Root cause. Reprocessing. Backfill. Safe re-run. Incident note, also called a post-mortem | `data-pipeline/incremental-processing-and-backfills`, `airflow/backfills-catchup-retries-and-sla` |

### Words this project cannot build, and how it still teaches them

Do not pretend. Teach these by a short, honest demonstration, and say that it is one.

- **Great Expectations and Soda.** Not installed. In milestone 4, show one check written three ways, by hand, in dbt and as it would look in each tool, in a short table. The lesson: the tool changes, the reasoning does not.
- **A paging service such as PagerDuty, and Slack alerts.** Airflow sends each alert from a task callback to a local alerts page and a log file, so no account is needed. The callback is real; only the destination is local. Say what a real team would plug in.
- **A schema registry.** Named in milestone 3 with a link to the chapter. The learner writes a small contract file by hand.
- **Data lineage tools.** The learner draws which output depends on which table by hand in milestone 1. Tools are named, not built.
- **Machine-learned anomaly detection.** Milestone 2 uses a simple normal-day comparison. Say where that stops being enough.
- **Airflow's own SQL check operators.** The gates here run dbt tests. In milestone 4, show one check written a second way, with Airflow's SQL check operator, so the learner knows both shapes they will meet at work.

### Skills, beyond the words

By the end the learner can also:

- Look at a table they have never seen and say what its checks should be, with a reason for each.
- Say what a common check does **not** catch.
- Prepare data for a check: join it, total it, or compare it with a normal day.
- Decide whether a failure should stop the pipeline, set rows aside, or warn.
- Read a bad night and say what failed, who was affected and whether a reload is safe.
- Explain each decision in an interview, with a real example from Lotus Retails.

## How every milestone is built

The same standard as Daily Orders and Retail Profit Stack, so a reader who knows one project knows all three.

Each milestone is a **self-contained folder** under `src/`. It imports nothing from another milestone. Code is copied forward on purpose, so two folders can be compared.

```
src/<milestone>/
  README.md              reading order, and how to run it
  docs/                  BRD.md, HLD.md, LLD.md, each with a data flow diagram
  notebooks/             the lab: numbered notebooks, run in order
  <package>/             production code
  checks/                the check cards that exist so far, one .md per table
  dags/                  the Airflow DAG as it stands after this milestone
  dbt_project/           the dbt models and tests, from milestone 4
  tests/                 tests for the production code
  milestone_N_flow.html  the whole pipeline at this milestone, animated (see "Visual pages")
  milestone_N_recap.html an animated closing page
```

Each milestone is built in this order:

1. **Design documents**: BRD (who is hurt and what they need), HLD (where the new part sits in the pipeline), LLD (the exact checks, their SQL, their limits and what a failure does).
2. **Lab notebooks**: six to eight short ones, about ten minutes each. Do each new idea by hand once before a tool does it. Show every result beside the answer key. One idea per notebook. Each opens with where the reader is and ends with what they now know.
3. **Production code and tests**: the same logic rebuilt properly. **Every check has two tests**: one where the fault is planted and the check fails, and one on clean data where it stays silent.
4. **Flow page**, then **recap page**.

A milestone is not done until its flow page and its recap page exist and open correctly.

## Visual pages

**This is a requirement, not decoration.** A beginner understands a gate when they see a bad row stopped at it and a good row pass. Every milestone must include HTML pages that show the entire pipeline flow.

### The pages to build

| Page | Where | What it shows |
|---|---|---|
| Business page | `understand_business/lotus_retails.html` | One receipt, from the till to the bonus. Who uses which table, and what a wrong value does to each of them. Built before milestone 1 |
| Flow page, one per milestone | `src/<milestone>/milestone_N_flow.html` | The whole pipeline at that milestone, with rows moving through it and every gate drawn |
| Recap page, one per milestone | `src/<milestone>/milestone_N_recap.html` | What was asked, what was built, what was learned, what is still wrong |
| Check card page | `checks/check_cards.html` | Every table's check card side by side, growing as the project goes on |
| Live page | `pipeline_live/` | The finished pipeline, run by the learner, with buttons to plant each fault |

### What every flow page must do

1. **Show the entire flow, end to end.** Store files, raw tables, clean tables, the two outputs, and the people who read them, with the Airflow tasks that run each stage. A gate that does not exist yet is drawn as a dashed line marked "no gate yet".
2. **Use the same layout in every milestone.** Each stage stays in the same place. The part that is new is highlighted.
3. **Animate real rows.** Actual receipts with their store, product and amount. The numbers on the page are the numbers the code produces.
4. **Show the planted fault passing the standard checks, then being caught.** The reader watches a bad row walk through five green checks and reach the manager. Then the same run with this milestone's check, and the row stops.
5. **Show where a caught row goes.** Stopped, set aside in quarantine, or let through with a warning. From milestone 4 the quarantine table is a visible box on the page, and a blocked task is shown in the state Airflow gives it.
6. **Follow one planted case through the whole flow.** Let the reader pick a case and watch it at every stage.
7. **Name each check with its lens and its word.** Business, data or operational, with the word it teaches and its meaning in one or two short sentences. The words match `GLOSSARY.md`.
8. **Let the reader control it.** Step forward, step back, play, and "plant the fault again".
9. **End with what is still wrong.**

### How they are made

The same rules as Daily Orders: each page is one self-contained HTML file with no build step, the numbers are generated from the milestone's real outputs by a script, and a test fails when a page is out of date. Readable on a phone and a laptop, in light and dark mode, with the keyboard, and the animation can be paused. Study the pages of Daily Orders first and keep the same look and quality.

## The first two minutes

A visitor must be able to see it work before reading anything, and **before installing Docker**. So the first run does not use Airflow: from a fresh clone, two commands run the given pipeline for one day as a plain program and print the bonus report, with all five standard checks green. One more command runs the finished pipeline on the same day. It prints the same report with one store marked "do not pay yet", the rows set aside, and the reason.

That contrast is the pitch of the project: **the same data, the same day, green checks, and a wrong number.**

For this to work, every step of the pipeline must also run as a plain command, with no scheduler. Airflow then starts those same commands as tasks, as in Daily Orders. Airflow holds no business logic.

The README is written for a person who has never seen the project. It says in its first lines what this is, who it is for, what they need, and that it is free. It includes screenshots.

## For the website, later

D8Loop will present this project as a project page and one page per milestone, as it does for Daily Orders. Each milestone's README and recap must state clearly:

- Why this milestone exists, what the learner builds, and the outcome.
- The new words it introduces, each explained in one or two short sentences.
- Setup steps that are new, with commands for macOS, Linux and Windows.
- The numbers: notebooks, tests, checks, gates, tables, rows set aside.
- Three to five things learned, and the one line that is still wrong.
- Three short questions with their answers, each a small situation and not a definition. The website shows them as a quiz.

Also provide, as data files the website can read: the planted cases with their dates and amounts, and each table's check card. The project page will let a visitor pick a fault and see which check catches it.

Enable GitHub Pages, so the `.html` pages open as pages. Add an empty `.nojekyll` file at the root.

The gallery card currently tags this project "Intermediate". It is now a starter project. Tell the owner when the repository is ready, so the card can be changed.

## Writing rules

Write for a reader with basic English and no data background.

- Short sentences. Simple words. No long paragraphs.
- Explain a word before you use it.
- Show code with its input rows and its result. Never describe a result without showing it.
- State numbers only when the repository can reproduce them.
- No claim like "this check is better" without showing the fault it catches and the one it misses.
- **Keep every notebook short.** If a notebook needs more than about ten minutes to read, split it. There are only five milestones, so the notebooks carry the pace.

## Tools

Python 3.12 or later, uv, Postgres, Docker Compose, dbt (Postgres adapter), Apache Airflow, pandas, pytest. A small local web server for the live page and the alerts page. The same stack as Daily Orders, so a learner who does both installs it once.

### Setup must be written for someone who has never installed Docker

This is a starter project that needs Docker and Airflow in its first milestone. That is the hardest moment for a beginner, and most people who give up, give up here. So milestone 1's setup is **click by click**:

- Separate steps for macOS, Windows and Linux. On Windows, cover WSL 2 and Docker Desktop in order, with what each screen asks.
- After every step, one command that proves it worked, and what the learner should see.
- How much memory and disk Docker needs, how long the first Airflow build takes, and that the wait is normal.
- The five most likely errors, each with its message and its fix: Docker is not running, a port is already in use, not enough memory, the build stopped halfway, Airflow's page does not open.
- How to stop everything, and how to free the disk space afterwards.

Follow the setup section of Daily Orders' milestone 5 and its "Shut everything down" section, and make this one more detailed. Test the steps on a clean machine if you can; if you cannot, say so.

Before you rely on a feature, check the current documentation: the Airflow version and its Docker setup, how a task callback is written in that version, how a backfill is started, which test features the dbt Postgres adapter supports, and how it saves failing rows.

## Scope boundaries

- No building of the pipeline itself. It is given. Building one is Daily Orders.
- No teaching of Airflow beyond what the gates, alerts and backfill need. Its full story is in Daily Orders and in the Airflow knowledge base. Link to them.
- No cloud account. Everything runs locally and costs nothing.
- No streaming. Checks on events as they arrive belong to the clickstream project.
- No lake or table formats. They belong to the lakehouse project.
- No dashboard. Outputs are tables and a printed report.
- No dedicated quality tool, no paging service, no schema registry. They are shown, not installed.
- No AI. It belongs to Retail Profit Stack.

All data is fictional. The project does not describe any real company's systems or data.

## How to work

- Build one milestone at a time. Never the whole project up front.
- Start by proposing three things, and stop for the owner's approval: the columns of the seven tables, the planted cases with their dates and amounts, and the hand-worked answer key.
- Then build the source system, the starter pipeline, its Airflow DAG and its five standard checks, and show that every planted case passes all five. Stop for approval again. If a planted case fails a standard check, the case is wrong, not the check.
- For each milestone, write the three short design documents first and stop for approval before notebooks or code.
- **Five milestones is a limit set by the owner.** Do not add a sixth. If a milestone has too much in it, move an idea to a neighbour or make the notebooks shorter, and tell the owner.
- When repository reality and this document disagree, say so and ask.
