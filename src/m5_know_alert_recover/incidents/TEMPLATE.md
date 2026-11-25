# Incident note: <one line that says what went wrong>

An incident note is a short written record of an incident. It is also called a post-mortem. Write it the same day, while you remember. It blames nobody. It says what happened and what was changed.

Keep it to one page.

| | |
|---|---|
| **Day of the data** | The business day that was wrong, like 2026-08-20 |
| **Found** | When, and by which gate or alert |
| **Fixed** | When the published number was right again |
| **Written by** | |

## What happened

Two or three sentences, in the words of the business. Which store, which day, which number.

## Who was hurt, and how much

Name the person or the team. Give the amount: dollars, bottles, emails. Say what they saw: a wrong number, or "do not pay yet".

## How we found out

Which gate caught it, and what it did: blocked, set rows aside, refused a file, or warned. Who was told, and at which level. If a person noticed before the pipeline did, say so. That is the most important line of the note.

## Root cause

The first thing that went wrong, the one that made the others happen. Not "a check failed": the check is how you found out. Ask "why?" until the answer is something a person can change.

## What we did

In order. The fix at the source. The night that was run again. The proof that the number is right: the published number beside the answer key, or beside the tills' own total.

## What we changed so it cannot happen silently again

A new check, a tighter limit, a line in the contract, a section in the runbook. If nothing needed to change because a gate already caught it on the night, say that. It is a good result.
