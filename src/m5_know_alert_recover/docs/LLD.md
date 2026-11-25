# LLD: Milestone 5, Know, alert, recover

**Status:** Final; built
**Fits inside:** [HLD](HLD.md)

> **What this document is.** A low-level design (LLD) gives the exact detail of each part named in the [HLD](HLD.md): the files, the tables, the alert rules, the commands and the tests. Keep it open beside the notebooks and the code. Section 11 says which notebook and which file build each part.

## 1. Files

Everything is inside `src/m5_know_alert_recover/`. Nothing imports from another milestone. The code is copied forward from milestone 4.

```
docs/                     BRD.md, HLD.md, LLD.md
notebooks/                the lab: eight notebooks, run in order
RUNBOOK.md                new: what each alert means, and the first things to check
incidents/                new: TEMPLATE.md, and the notes written in the lab
dbt_project/              as before, with two more tests
  tests/landing/file_arrived_on_time.sql    new: freshness
  tests/clean/impossible_price.sql          new: a price 100 times the list
lotus_pipeline/           production code
  history.py              new: one row per step of every run
  health.py               new: four health numbers, each with a target
  alerts.py               new: who is told, how urgently, and what to do
  load.py                 changed: a corrected file is loaded again
  pipeline.py             changed: nine steps, and every step is remembered
  flow_data.py            builds the numbers shown on the flow page and the recap page
dags/lotus_nightly_dag.py changed: callbacks. The DAG is lotus_nightly_m5
airflow_tools.py          new helpers: backfill, stores, alerts_log
tests/                    tests for all of it
```

The live page of the finished pipeline, and the alerts page, are in [`pipeline_live/`](../../../pipeline_live/) at the project root.

## 2. Know: run history (`history.py`)

`ops.run_history`: one row per step of every run. Rows are only added, never removed.

| Column | Example |
|---|---|
| `run_day` | `2026-08-24`: the business day the run was for |
| `step` | `gate-clean` |
| `status` | `ok`, `blocked` or `error` |
| `started_at` | The real clock: when you ran it |
| `seconds` | `1.42` |
| `detail` | The first line the step printed |

A night that was run again has two sets of rows. A blocked step is in the history too, with `blocked`.

| Function | Returns |
|---|---|
| `record(wh, day, step, status, started_at, detail)` | Writes one row |
| `last_run_of_each_night(wh, first, last)` | For each night: did its newest run publish, how long it took, how many runs it has had |

Airflow's own history, from the project folder:

```sh
docker compose exec airflow airflow dags list-runs lotus_nightly_m5
docker compose exec airflow airflow tasks states-for-dag-run lotus_nightly_m5 night_2026-08-24
```

## 3. Know: the four health numbers (`health.py`)

```sh
uv run python -m lotus_pipeline health --day 2026-08-30
```

```
Is the pipeline healthy? Week of Mon 2026-08-24, up to Sun 2026-08-30

  ok   Did it run?              7 of 7 nights published
  ok   Did it finish on time?   the slowest night took 3 seconds. Target: under 300
  MISS Is the data fresh?       39 of 41 sales files arrived by 01:00: 95.1%. Target: 98%
         late:    S06 2026-08-27, 23.7 hours after it was due
         missing: S04 2026-08-25
       Did every store report?  36 of 41 store days can be paid from: 87.8%
```

The seconds depend on your computer. Every other number is the same for everyone.

| SLI | How it is measured | SLO, in `config.py` |
|---|---|---|
| Nights that published | The newest run of each night in `ops.run_history` has a `publish` step with `ok` | All of them |
| Seconds a night took | The sum of its steps | `NIGHT_SECONDS_TARGET = 300` |
| Files on time | A sales file expected for an open store's day, whose `received_at` is not after 01:00 the next morning | `FILES_ON_TIME_TARGET = 98` |
| Store days that can be paid from | `output.store_day_status` with status `ok` | None. Shown |

41 files are expected: six stores, seven days, less one planned refit.

### The freshness test

`dbt_project/tests/landing/file_arrived_on_time.sql`, a warning at the landing gate:

```sql
SELECT file_name, substr(file_name, 13, 3) AS store_id, file_day AS sales_date, received_at,
       CAST(file_day + 1 AS TIMESTAMP) + INTERVAL '1 hour' AS due_at
FROM (the store files, each with the day in its name) files
WHERE received_at > CAST(file_day + 1 AS TIMESTAMP) + INTERVAL '1 hour'
```

It returns a file only once the file has arrived. A file that never came has no row, and a row that is not there cannot be late. So two checks share the work:

| | Thursday night: `S06`'s file is not there | Friday night: it has arrived |
|---|---|---|
| Every store has a day we can pay from | **Fails:** no sales file arrived | Passes |
| A store's file arrives before 01:00 | Passes: nothing to judge | **Fails:** 23.7 hours late |

dbt has its own command, `dbt source freshness`. It compares the newest row with the clock on the wall. This project's data is from August 2026, so that command would call every file months late. The test above compares with the night's own clock.

## 4. Alert: what an alert is (`alerts.py`)

| Field | Example |
|---|---|
| `fingerprint` | `store_day|S04|2026-08-25`. The same problem tomorrow has the same fingerprint |
| `level` | `page`, `ticket` or `message` |
| `to_whom` | `Head of retail operations` |
| `title` | `S04 Tue 08-25: No sales file arrived` |
| `how_bad` | `The receipts and the tills are $587.42 apart. Ines Marchetti's bonus waits.` |
| `what_to_do` | `Ask the store's back office to send the file. Do not pay this store's bonus yet.` |
| `runbook` | `ask-a-store-for-its-file`, a section of [RUNBOOK.md](../RUNBOOK.md) |

### Where alerts come from

| Source | Becomes | Level | To |
|---|---|---|---|
| A failed test whose action is `block` | One alert per test | Page | The data engineer on call |
| A task that crashed | One alert | Page | The data engineer on call |
| A row of `store_day_status` that is not `ok` | One alert per store and day, with the reasons of every row set aside | Ticket | Head of retail operations |
| A refused file that is not a sales file | One alert per file | Ticket | Finance and the buying team, for a supplier's file |
| A failed warning or quarantine test of another table | One alert per store and day, file, product or member | See `WARNINGS` | The person named there |

Row tests on `sales_lines` have no alert of their own. What they set aside reaches a person through the store's day. They are listed in `COVERED`. A test goes through every dbt test and fails if one is neither routed nor covered.

### Sending (`send`)

1. Work out every problem that exists after tonight: `tonight(wh, day)`.
2. Compare with the open alerts in `ops.alerts`, by fingerprint.
3. **New:** store it. Send it, unless it is a message.
4. **Still open:** do not send. Count it.
5. **Gone:** mark it resolved, and send one "RESOLVED" line.

| Table | Holds |
|---|---|
| `ops.alerts` | One row per problem: the alert, the first night, the last night it was seen, the night it was resolved |
| `ops.alert_log` | One row per alert per night: `new`, `still open` or `resolved` |
| The alerts log, a file | One line per alert that was sent |

One line of the log:

```
2026-08-26 02:00  TICKET  to Head of retail operations: S04 Tue 08-25: No sales file arrived. The receipts and the tills are $587.42 apart. Ines Marchetti's bonus waits. Do: Ask the store's back office to send the file. Do not pay this store's bonus yet. (RUNBOOK.md#ask-a-store-for-its-file)
```

### The bonus week, night by night

| Night | Sent | What |
|---|---|---|
| Mon 08-24 | 1 | `S03`: rows set aside |
| Tue 08-25 | 2 | `SUP-B`'s file refused. `S04`: no sales file |
| Wed 08-26 | 0 | One message in the digest: the same member twice |
| Thu 08-27 | 2 | `S05`: rows set aside. `S06`: no sales file |
| Fri 08-28 | 3 | A missing stock count. Yesterday's butter price. `SUP-C`'s file refused. And `S06` is **resolved** |
| Sat 08-29 | 1 | `S06`: rows set aside |
| Sun 08-30 | 1 | `S02`: rows set aside |
| **The week** | **10** | Against 70, if every failed test rang every night |

`shout(wh, day)` returns what the careless way would send. It exists to be counted.

## 5. Alert: the callbacks (`dags/lotus_nightly_dag.py`)

```python
def tell_someone(context) -> None:
    """Called by Airflow when a task fails, and when publish succeeds."""
    task = context["ti"].task_id
    day = context["ds"]
    failed = context["ti"].state == "failed"
    command = f"{PIPELINE} alerts --day {day}" + (f" --failed-task {task}" if failed else "")
    done = subprocess.run(command, shell=True, capture_output=True, text=True)
    print(done.stdout or done.stderr)
```

| Callback | On | Fires when | Result |
|---|---|---|---|
| `on_failure_callback` | Every task, through `default_args` | A gate blocks, or a step crashes | A page |
| `on_success_callback` | `publish` | The night is published | The tickets of the night |

A callback is used, and not a last task, for one reason: when a gate fails, the tasks after it are never started. A callback still fires.

Inside Docker the alerts log is `/var/lotus/alerts.log`. Read it with:

```sh
docker compose exec airflow tail /var/lotus/alerts.log
```

## 6. Recover: a corrected file is loaded again (`load.py`)

`raw.loaded_files` has one more column: `checksum`, a fingerprint of the file's bytes.

| The file | The load step |
|---|---|
| Is not in `raw.loaded_files` | Loads it |
| Is there, with the same fingerprint | Skips it. This is what makes a second run safe |
| Is there, with another fingerprint | Removes its old rows from the raw table, then loads it again |

```
load:         1 new files, 113 rows copied to the raw tables. 1 changed file loaded again.
```

## 7. Recover: one night, run again

At the source. The stores are a small program, so "the store sends a corrected file" is one command. Under Airflow it runs inside the container, where Airflow's stores live:

```sh
docker compose exec -w /opt/lotus airflow /opt/lotus-venv/bin/python -m store_systems fix --case 1
```

Then run only that night again, with Airflow's backfill:

```sh
docker compose exec airflow airflow backfill create --dag-id lotus_nightly_m5 \
    --from-date 2026-08-24 --to-date 2026-08-25 --reprocess-behavior completed --max-active-runs 1
```

| Part | Meaning |
|---|---|
| `--from-date`, `--to-date` | The range. A night's run is stamped 02:00 in Lotus's time zone, so the range ends the morning after |
| `--reprocess-behavior completed` | Run these nights again even though they already ran. Without it Airflow skips a night that has a run |
| `--max-active-runs 1` | One night at a time, oldest first |

Without Airflow, the same recovery is two plain commands:

```sh
uv run python -m store_systems fix --case 1
uv run python -m store_systems deliver --day 2026-08-24
uv run python -m lotus_pipeline run --day 2026-08-24
```

What the night does, step by step, for the refund at `S03`:

| Step | Before the fix | After the fix, same night run again |
|---|---|---|
| load | Nothing new | 1 changed file loaded again |
| gate_clean | 1 row set aside: a refund gives money back | Nothing set aside |
| publish | `S03` Monday: $786.69, "do not pay yet" | $777.20, `ok` |
| alerts | 1 ticket | The ticket is RESOLVED |
| The answer key | $777.20 | $777.20 |

## 8. The four drills

Each is run on Thursday 2026-08-20, a clean day, so it stands alone. `python -m store_systems fault <kind> --store <store> --day 2026-08-20` switches the fault on.

| Drill | Fault | Which gate caught it | What was set aside | What the manager saw | Who was told | The fix |
|---|---|---|---|---|---|---|
| 1 | `file-twice`, `S03` | `gate_clean` | Every line of the second file, as extra copies | The right number, and "yes" | Nobody. A message in the digest | None. The gate removes the copies every night |
| 2 | `rename-column`, `S04` | `gate_landing` | The whole file, refused | "Do not pay yet: the sales file was refused. An expected column is missing: amount_cents" | A ticket to the head of retail operations | `fix --all-faults`, and the night again |
| 3 | `late-file`, `S05` | `gate_publish` | Nothing | "Do not pay yet: no sales file arrived" | A ticket, closed the next night | None. It arrives. Freshness names it: 23.7 hours late |
| 4 | `bad-prices`, `S06` | `gate_clean` | A tenth of the lines, then the whole file: more than 5% was bad | "Do not pay yet", with the rows | A ticket | `fix --all-faults`, and the night again |

After each fix the published number equals the answer key, and the status is `ok`.

### The second new test

`dbt_project/tests/clean/impossible_price.sql`: the price charged is at most twice the price list, and at least half of it. Rows that break it are set aside. It stands beside "the price charged is the price valid that day", which only warns: a store that charges yesterday's price took real money. A price 100 times the list is a broken file.

## 9. The Monday answer

Four stores send corrected files: cases 1, 3, 5 and 6. Their four nights are run again: Monday, Thursday, Saturday and Sunday. `S04`'s Tuesday file never arrives.

```
  Store                           Manager              Net sales  Bonus (1%)   Pay?
  S01  Lotus Ashby High Street    Marta Lindqvist      $5,153.25      $51.53   yes
  S02  Lotus Ashby Market Square  Dana Okafor          $5,748.63      $57.49   yes
  S03  Lotus Calder Bridge        Tomas Reyes          $5,087.62      $50.88   yes
  S04  Lotus Calder Station       Ines Marchetti       $4,585.57           -   DO NOT PAY YET
         Tue 08-25: no sales file arrived. The tills counted $587.42
  S05  Lotus Fenwick Green        Samir Haddad         $3,593.05      $35.93   yes
  S06  Lotus Fenwick Harbour      Aiko Tanaka          $4,418.89      $44.19   yes

  5 of 6 stores can be paid.
```

Each of the five numbers equals the answer key. One ticket is open: `S04 Tue 08-25: No sales file arrived`.

## 10. Tests

| Test file | What it proves |
|---|---|
| `test_know_alert_recover.py` | **Know:** every step of every night is in the run history. The four health numbers of the bonus week. Freshness sees the late file only once it has arrived. **Alert:** ten tickets in the week, never one twice. Shouting would send 70. Each alert goes to the person who can act, and says what, how bad and what to do. No alert carries personal data. A blocked gate pages the engineer on call. Every dbt test is routed or covered. Every runbook section an alert names exists. **Recover:** a file that did not change is skipped, a corrected one is loaded again and is there once. The four drills. The Monday answer: five stores, each equal to the answer key, and `S04` |
| `test_gates.py`, `test_dbt_project.py` | Milestone 4's gates, unchanged in what they do |
| `test_dag.py` | The callbacks are on every task and on publish. The real Airflow reads the DAG |
| The others | The code copied forward still works |
| `test_notebook_parity.py` | The answer the notebooks saved equals the production answer |
| `test_flow_page.py` | Every number on the flow page and the recap page comes from the code |

## 11. Where each part is built

| Notebook | Teaches | Design sections | Code |
|---|---|---|---|
| `01_did_it_run` | Run history | HLD 2; LLD 2 | `history.py` |
| `02_on_time_as_a_number` | Freshness; SLI, SLO and SLA | BRD 4; HLD 2; LLD 3 | `health.py`, the freshness test |
| `03_five_signals` | Volume, observability | HLD 2 | |
| `04_tell_the_person_who_can_act` | Alert, callback, routing, runbook | BRD 5, 6; HLD 3; LLD 4, 5 | `alerts.py`, `dags/`, `RUNBOOK.md` |
| `05_too_many_alerts` | Alert fatigue | BRD 7; HLD 3; LLD 4 | `alerts.py` |
| `06_fix_one_day` | Incident, root cause, reprocessing, backfill, safe re-run | BRD 8; HLD 4; LLD 6, 7 | `load.py`, `airflow_tools.py` |
| `07_four_drills` | Incident note | HLD 4; LLD 8 | `incidents/` |
| `08_the_monday_answer` | The whole project, finished | LLD 9, 10 | `pipeline.py`, `tests/` |

## 12. What is still wrong

Nothing.

The question was: can I pay the bonus from last week's net sales, store by store? And if I cannot trust a number, which one, and why?

The answer is: yes for five stores, no for one, and here is the row that says why.
