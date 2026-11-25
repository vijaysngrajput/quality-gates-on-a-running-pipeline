# Runbook

A runbook is a short note that says what an alert means and the first things to check. Every alert of this pipeline ends with a link to one section of this page.

Read the alert first. It already says which store, which day and how much money.

## A gate blocked

**What it means.** A test whose failure must stop the pipeline has failed. Nothing new was published. Every reader still sees the last good numbers. Level: **page**.

**First things to check.**

1. Which test? `SELECT * FROM ops.gate_results WHERE status <> 'pass' AND action = 'block'`.
2. Which rows? dbt kept them: `SELECT * FROM audit.<test name>`.
3. Is it a reference table (`stores`, `products`, `till_totals`)? Then every number would have been wrong. Look at the newest file of that table in the landing folder.
4. Is it "the receipts add up to the till's own total, or we can say why not"? Then a fault exists that no check knows. Do **not** publish by hand. Find the row first.

**To recover.** Fix the table or the file at its source. Run the night again: `airflow backfill create --dag-id lotus_nightly_m5 --from-date <day> --to-date <day after> --reprocess-behavior completed`.

## A task failed

**What it means.** A step stopped with an error. It is not a failed check: the program itself broke. Nothing new was published. Level: **page**.

**First things to check.**

1. The log of the task, on its page in Airflow.
2. Is the warehouse up? `docker compose ps`.
3. Did the step before it finish? `SELECT * FROM ops.run_history ORDER BY started_at DESC`.

**To recover.** Fix the cause and run the night again. Every step is safe to run twice.

## Ask a store for its file

**What it means.** An open store has no sales file, or no till totals, for a day. The other stores were published. This store's day is marked "do not pay yet". Level: **ticket**.

**First things to check.**

1. Was the store open? The calendar says: `SELECT * FROM clean.calendar`.
2. Is the file only late? `python -m lotus_pipeline health --day <day>` lists late files and missing files apart.
3. Did the tills send their totals? Then the store was trading, and only the export failed.

**What to do.** Ask the store's back office to send the file. When it arrives, the next night loads it by itself, and this alert is closed. Do not pay the store's bonus for that week before then.

## Rows were set aside

**What it means.** Some rows of a store's file broke a rule and were set aside. Without them the day does not add up to the tills' own total. Level: **ticket**.

**First things to check.**

1. The rows and the reason: `SELECT * FROM quarantine.sales_lines WHERE store_id = '<store>'`.
2. The size of the gap: `SELECT * FROM output.store_day_status WHERE status <> 'ok'`.
3. Is the whole file set aside, with the reason "More than 5% of this file was set aside"? Then the export is broken, not one row.

**What to do.** Ask the store for a corrected file. Never repair a row by hand in the warehouse: the next night would bring the bad row back. When the corrected file arrives, run that night again.

## A file was refused

**What it means.** A whole file was set aside when it landed: an agreed column is missing, or a supplier's file breaks the contract. The last good file is in use. Level: **ticket**.

**First things to check.**

1. Which file, and why: `SELECT * FROM ops.refused_files`.
2. For a supplier's file: `python -m lotus_pipeline contract --file <path>` lists every broken rule.

**What to do.** Send the reason to the sender and ask for a file that keeps the agreement. For a supplier, the contract is `contracts/supplier_prices.yml`. Nothing needs to be undone: the refused file was never used.

## A warning

**What it means.** A rule is broken, and the published numbers are still right. Level: **ticket** when money or an order is involved, **message** when it is only news.

| The alert says | It means | The first thing to do |
|---|---|---|
| charged a price that is not in the price list | A store's tills have an old price. The sales number is right. The store took less or more than it should | Ask the store to update its tills |
| has a product with no stock count | Nobody counted one shelf. The order list says "no count: order by hand" | Count it, or order by hand |
| is the same person as an older member | One person, two accounts. Only the older one gets email | Merge the accounts |
| had a day far from its normal day | A reason to look. Not a proof of a fault | Read the other alerts for that store |
| sent its file late | The file is loaded. One night ran without it | Ask why, if it happens again |
| has a column nobody agreed | A sender added a column. Nothing breaks | Ask what it means |
| sent sales lines twice | The extra copies were removed, and the day adds up | Ask why the export ran twice |
