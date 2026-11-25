# Incident note: impossible prices at S06 on Thursday 2026-08-20

An incident note is a short written record of an incident. It is also called a post-mortem. It blames nobody.
This one was written in the milestone 5 lab, notebook 7, after a drill: the fault was planted on purpose.

| | |
|---|---|
| **Day of the data** | Thursday 2026-08-20 |
| **Found** | The run of that night, 02:00 on 2026-08-21, by the clean gate: the test `impossible_price` |
| **Fixed** | The same day, by a backfill of the night 2026-08-20 |
| **Written by** | The Lotus data team, in the lab |

## What happened

Store S06 sent its sales file for Thursday 2026-08-20. In 12 of its 118 lines the price charged was a hundred times the price list.
Those 12 lines alone came to $6,300.00. The answer key, added up from the receipts, says the whole day was $778.59.

## Who was hurt, and how much

Aiko Tanaka, the manager of S06. Her day was marked "do not pay yet" for one night, so her bonus waited.
Nobody saw a wrong number: the bad lines never reached the published tables.

## How we found out

The clean gate. The test `impossible_price` set 12 lines aside. Because more than 5% of the file was bad,
the whole file was then set aside: 106 more lines, with the reason `too_many_bad_rows`.
The head of retail operations got a ticket on the night. Finance got a second ticket from the warning `price_of_the_day`.
No person noticed before the pipeline did.

## Root cause

The store's export program wrote some prices in the wrong unit, a hundred times too large.
The check is how we found out. The export is what went wrong.

## What we did

1. The store sent a corrected file under the same name.
2. We ran only the night of 2026-08-20 again, with an Airflow backfill. The load step saw a new fingerprint and loaded the file again.
3. Proof: S06 on 2026-08-20 is published as $778.59. The answer key says $778.59. The status is `ok`. Both tickets were resolved.

## What we changed so it cannot happen silently again

Nothing needed to change in the checks: the gate caught it on the night, and that is a good result.
One thing to improve: the warning `price_of_the_day` also sent a ticket to finance, saying the sales number was right.
For a price that is impossible, that ticket is noise. A test that already sets a row aside should silence the warning on the same row.
