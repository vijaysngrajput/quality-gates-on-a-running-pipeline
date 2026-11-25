"""The cases of the milestone 5 flow page: stopped, told, and made right.

flow_data.py runs the bonus week. This file runs what comes after it, each on its own
private warehouse and landing folder:

    the recovery   four stores send a corrected file, and four nights are run again
    four drills    a fault switched on in the stores on a clean Thursday, then fixed
    one block      a store deleted from the stores table, to see a page go out

and it tells each case as steps for the page.
"""

import shutil
from datetime import date, timedelta
from pathlib import Path

from lotus_pipeline import alerts
from lotus_pipeline.flow_data import (
    DRILL_DAY,
    EMPTY,
    FRI,
    LAB_DAY,
    MON,
    SAT,
    SUN,
    TASKS,
    THU,
    TUE,
    WEEK,
    Case,
    _card,
    _copy,
    _count,
    _day,
    _line_card,
    _outputs,
    _people,
    _point_at,
    _read_csv,
    _rows,
    _run_night,
    _sales_case,
    _tasks,
    alert_cards,
    health_lines,
)
from lotus_pipeline.report import money
from lotus_pipeline.warehouse import connect
from store_systems import files, truth
from store_systems.catalog import BONUS_WEEK_START, LAST_DAY
from store_systems.generate import build as build_world

BACKFILL = (
    "docker compose exec airflow airflow backfill create --dag-id lotus_nightly_m5 \\\n"
    "    --from-date {day} --to-date {after} --reprocess-behavior completed"
)

# How each of the four stores puts its fault right, at the source.
FIXES = {
    1: (MON, "The store's back office runs the export again. The refund has its minus sign now."),
    3: (THU, "The export program is corrected. The scale's 1,180 grams are written as 1.180 kg."),
    6: (SAT, "The buying team types Pumpkin soup into the price list. The sales file itself "
             "was right all along."),
    5: (SUN, "The till's clock is set right. The receipt is stamped 21:40 on Sunday again."),
}  # fmt: skip


# ---------------------------------------------------------------- running what comes after the week


def recover_the_week(temp: Path, landing: Path) -> dict:
    """Fix four planted cases at the source, one by one, and run only that night again."""
    fixed_landing = temp / "landing_fixed"
    shutil.copytree(landing, fixed_landing)
    _copy(temp / "week", temp / "recover")
    _point_at(temp / "recover", fixed_landing)
    recovered = {}
    for number, (day, fix) in FIXES.items():
        state = files.read_state(fixed_landing)
        state["fixed"].append(number)
        files.write_state(fixed_landing, state)
        files.deliver_through(LAST_DAY, fixed_landing)  # the stores send it right this time
        night, through = _run_night(day), day
        if number == 6:
            # The price list arrives whole every night, and the pipeline uses the NEWEST copy
            # it has loaded. So Saturday is only right once Sunday's copy is loaded again too.
            first = night["load"]["loaded_again"]
            night, through = _run_night(SUN), SUN
            night["load"]["loaded_again"] = first + night["load"]["loaded_again"]
        recovered[number] = {"night": night, "fix": fix, "landing": fixed_landing, "day": day,
                             "through": through}  # fmt: skip
    return recovered


DRILLS = {
    "file-twice": ("S03", "A store's file arrives twice"),
    "rename-column": ("S04", "A column is renamed"),
    "late-file": ("S05", "A file is a day late"),
    "bad-prices": ("S06", "A batch of impossible prices"),
}


def run_the_drills(temp: Path) -> dict:
    """The four drills of tests/test_know_alert_recover.py, each in its own small world."""
    key = truth.daily_store_sales(build_world())
    iso = DRILL_DAY.isoformat()
    drills = {}
    for kind, (store, name) in DRILLS.items():
        folder = temp / f"drill_{kind}"
        landing = folder / "landing"
        files.write_state(
            landing, {"faults": [{"kind": kind, "store": store, "day": iso}], "fixed": []}
        )
        files.deliver_through(DRILL_DAY + timedelta(days=1), landing)
        _point_at(folder, landing)
        first = _run_night(DRILL_DAY)
        sent_file = _read_csv(landing, "sales_lines", f"sales_lines_{store}_{iso}.csv")
        if kind in ("rename-column", "bad-prices"):  # the store sends a corrected file
            files.write_state(landing, {"faults": [], "fixed": []})
            files.deliver(DRILL_DAY, landing)
            second = _run_night(DRILL_DAY)
        elif kind == "late-file":  # nothing to fix: the file arrives by itself, a day late
            second = _run_night(DRILL_DAY + timedelta(days=1))
        else:  # nothing to fix: the gate removes the copies again, every night
            second = _run_night(DRILL_DAY)
        right = key[(DRILL_DAY, store)]
        published = second["sales"].get((iso, store), {}).get("net_sales_cents")
        status = first["status"][(iso, store)]
        sent = first["alerts"]["sent"]  # the ticket about the store's day first, if there is one
        tickets = [a["title"] for a in sorted(sent, key=lambda a: a["to"] != alerts.OPERATIONS)]
        drills[kind] = {
            "kind": kind, "name": name, "store": store, "first": first, "second": second,
            "landing": landing, "sent_file": sent_file, "right": right,
            "gate": {"file-twice": "gate_clean", "rename-column": "gate_landing",
                     "late-file": "gate_publish", "bad-prices": "gate_clean"}[kind],
            "set_aside": (
                f"The whole file: {first['refused'][0]['file_name']}" if first["refused"]
                else _count(len({r["line_id"] for r in first["aside_lines"]}), "line")
                if first["aside_lines"] else "Nothing. There was nothing to set aside"
            ),
            "manager_saw": "yes: the day is right" if status["status"] == "ok" else "DO NOT PAY YET",
            "told": tickets[0] if tickets else "Nobody had to act. A message waits in the digest",
            "fixed": {"file-twice": "No fix. The gate removes the copies every night",
                      "rename-column": "The store sent the file again, with the agreed columns",
                      "late-file": "No fix. The file arrived by itself the next night",
                      "bad-prices": "The store sent the file again, with the real prices"}[kind],
            "right_again": published == right["net_sales_cents"]
            and second["status"][(iso, store)]["status"] == "ok",
        }  # fmt: skip
    return drills


# ---------------------------------------------------------------- pieces every case uses


def _stage(count: str, *cards: dict) -> dict:
    return {"count": count, "cards": list(cards)}


def _alert_facts(a: dict) -> list:
    return [
        ["Level", a["level"]],
        ["To", a["to"]],
        ["What", a["title"]],
        ["How bad", a["how_bad"]],
        ["What to do", a["do"]],
        ["Runbook", f"RUNBOOK.md#{a['runbook']}"],
    ]


def _told(case: Case, night: dict, about: str, word: str, stages: dict, extra: str = "") -> None:
    """The third act begins: tonight's alerts go out, and one of them is about this case."""
    found = night["alerts"]
    mine = [a for a in found["sent"] if about in a["title"]]
    case.alerts = alert_cards(found, about)
    case.history = night["history"]
    if not mine:
        return
    a = mine[0]
    case.add(
        "told", "alerts", f"02:00, the same night: a {a['level']} goes to {a['to']}",
        "This is new. When the publish task succeeds, Airflow calls a callback: a function it "
        "runs by itself. The callback starts one command, python -m lotus_pipeline alerts. That "
        "command decides who must know. It writes the alert to a table, to a log file and to the "
        f"task's log. Tonight: {case.alerts['counts']}. " + extra,
        word, "night", _tasks(8), stages, tone="caught", callback="publish", facts=_alert_facts(a),
    )  # fmt: skip


def _history_step(case: Case, night: dict, stages: dict, text: str, word: str = "Run history",
                  clock: str = "morning") -> None:  # fmt: skip
    case.history = night["history"]
    case.health = health_lines(night["health"]) if "health" in night else []
    case.add("history", "history", "What the pipeline remembers, and how it measures itself",
             text, word, clock, _tasks(8), stages)  # fmt: skip


# ---------------------------------------------------------------- a planted case, and its recovery


def _recovered_case(cid: str, night: dict, landing: Path, key: dict, rec: dict, *, words: list[str],
                    **facts) -> dict:  # fmt: skip
    """Three acts: the gates stop it, someone is told, and the fixed night comes back right."""
    case = _sales_case(cid, night, landing, key, **facts)
    f = case.facts
    store, day, right, manager = f["store"], f["day"], f["right"], f["manager"]
    iso = day.isoformat()
    shown = {"outputs": f["outputs"], "people": f["people"]}
    _told(case, night, f"{store} {_day(day)}", words[0], shown,
          "It names the store, the day and the money, and it says what to do first.")  # fmt: skip

    again = rec["night"]
    _c, fixed_rows = _read_csv(rec["landing"], "sales_lines", f"sales_lines_{store}_{iso}.csv")
    was_bad = [r for r in fixed_rows if r["line_id"] in f["bad_ids"]][:3]
    soup = cid == "6"
    cards = [_line_card(r, "good", "as the store sends it now") for r in was_bad]
    if soup:
        cards = [
            _card("Pumpkin soup 500ml", "$5.99", "new in the price list, from Sat 08-29", "good")
        ]
    case.add(
        "fix", "files", "The morning after: find the root cause, and fix it at the source",
        f"{alerts.OPERATIONS} reads the ticket and calls the store. {rec['fix']} Nobody touches a "
        "row in the warehouse. A row repaired by hand would be bad again tomorrow, because the "
        "clean tables are built from the raw rows every night.",
        words[1], f"{_day(day + timedelta(days=1))}, 09:00", _tasks(0, "stores_send_files"),
        {"files": _stage("The price list, as typed now" if soup else f"{len(fixed_rows)} lines from {store}", *cards)},
        plant=False,
    )  # fmt: skip

    loaded = again["load"]["loaded_again"]
    mine = [name for name in loaded if store in name] or loaded[:1]
    through = rec["through"]
    note = (
        f" The price list arrives whole every night, so {len(loaded)} files have a new "
        "fingerprint. And the pipeline uses the newest copy it has loaded: so this backfill "
        f"covers two nights, {_day(day)} and {_day(through)}."
        if soup
        else ""
    )
    case.night, case.gates_run = again, []
    case.gate_state = {"landing": "", "clean": "", "publish": ""}
    for gate in ("landing", "clean", "publish"):
        case.ran(gate)
    case.history = again["history"]
    runs = next(h["runs"] for h in again["history"] if h["day"] == _day(day))
    case.add(
        "rerun", "raw",
        f"Run only that night again: a backfill of {_day(day)}" if through == day
        else f"Run only those nights again: a backfill of {_day(day)} and {_day(through)}",
        f"One Airflow command runs the night of {_day(day)} again, all eight tasks. The load step "
        "keeps a fingerprint of every file it loaded. A file with the same fingerprint is skipped. "
        f"A file that changed is loaded again, and its old rows go first: {mine[0]}.{note} The run "
        f"history now shows {runs} runs for that night.",
        words[2], f"{_day(day + timedelta(days=1))}, 09:30", _tasks(8),
        {"raw": _stage(f"{_count(len(loaded), 'changed file')} loaded again", *cards)},
        sql=BACKFILL.format(day=iso, after=(through + timedelta(days=1)).isoformat()),
        moves=[{"from": "files", "to": "raw"}],
    )  # fmt: skip

    left = [r for r in again["aside_lines"] if r["store_id"] == store and r["file_day"] == iso]
    left += [r for r in again["aside_raw"] if r["store_id"] == store and r["file_day"] == iso]
    case.aside = _stage(f"Nothing from {store} on {_day(day)} any more" if not left
                        else f"{_count(len(left), 'row')} still set aside")  # fmt: skip
    case.alerts = alert_cards(again["alerts"], f"{store} {_day(day)}")
    published = int(again["sales"][(iso, store)]["net_sales_cents"])
    outputs, people = _outputs(again, store, day), _people(again, store)
    new_tickets = [a for a in again["alerts"]["sent"] if "no stock count" in a["title"]]
    extra = (
        f" One thing more: a product that is new on the list has no stock count yet, so "
        f"{_count(len(new_tickets), 'ticket')} went to the replenishment team. A fix can show "
        "the next thing to do."
        if new_tickets
        else ""
    )
    case.add(
        "right", "people", f"{store} {_day(day)}: {money(published)}, the same as the answer key",
        f"The gates run again on the corrected rows, and nothing of {store}'s day is set aside. "
        f"The published number is {money(published)}. The answer key says {money(right)}. "
        f"{manager['manager_name']} can be paid for that day, and the ticket is closed: RESOLVED. "
        "The last thing to do is to write the incident note: what happened, who was hurt, why, "
        "and what was changed." + extra,
        words[3], f"{_day(day + timedelta(days=1))}, 09:31", _tasks(8),
        {"outputs": outputs, "people": people}, moves=[{"from": "outputs", "to": "people"}],
    )  # fmt: skip
    case.data["caught_by"] = case.data["caught_by"] + [
        f"told · {next((a['to'] for a in night['alerts']['sent'] if store in a['title']), '')}",
        "recovered · backfill of one night",
    ]
    case.data["recovered"] = {"published": money(published), "answer_key": money(right),
                              "equal": published == right}  # fmt: skip
    case.data["outcome"] = f"Told on the night. Fixed at the source. {money(published)} again"
    return case.finish()


# ---------------------------------------------------------------- cases 8 and 7: late, and never


def _file_case(cid: str, night: dict, later: dict, key: dict, *, store: str, day: date,
               name: str, hint: str) -> Case:  # fmt: skip
    iso = day.isoformat()
    right = key[(day, store)]["net_sales_cents"]
    manager = next(s for s in night["report"]["stores"] if s["store_id"] == store)
    case = Case(
        cid, night, label=f"Case {cid}", store=store, hint=hint, name=name,
        where=f"{_day(day)} · store {store}, {manager['store_name']}", table="sales_lines",
        output="daily_store_sales", says_label="Before the gates, the report said",
        key_label="The answer key says", pipeline_says="no row at all",
        answer_key_says=money(right), caught_by=["gate_publish · store is not ready to pay · warning"],
    )  # fmt: skip
    files_stage = _stage(
        f"5 files, none from {store}",
        _card(f"sales_lines_{store}_{iso}.csv", "", "it did not arrive", "bad"),
        _card(f"till_totals_{store}_{iso}.csv", money(right), "the tills counted this", "good"),
    )
    case.add(
        "plant", "files", f"{_day(day)}, 01:00 passes, and {store} has sent no sales file",
        f"The other five stores sent theirs. {store}'s tills sent their totals, {money(right)}, "
        "so the store was open and selling. Only the sales file is not there.",
        "Freshness", "evening", _tasks(0, "stores_send_files"), {"files": files_stage}, plant=True,
    )  # fmt: skip
    case.add(
        "before", "people", "First, the old run: five green checks, and no row to fail",
        "This is milestone 1 again. A row that is not there cannot fail a check. The table is not "
        f"empty, because five stores sent lines. So the report has no number for {store} on "
        f"{_day(day)}, and nothing says so.",
        "Generic test", "morning", _tasks(0),
        {"files": files_stage,
         "outputs": _stage("daily_store_sales, with no gate",
                           _card(f"{store} · {_day(day)}", "–", "no row", "bad"),
                           _card("Answer key", money(right), "from the till journal", "good")),
         "people": _stage("The old report", _card(f"{store} · {manager['manager_name']}", "",
                                                  "a day is missing. Checks: 5 of 5 passed", "bad"))},
        tone="problem",
    )  # fmt: skip
    for gate in ("landing", "clean"):
        case.ran(gate)
    found = case.ran("publish")
    stored = [r for r in found["stored"]["store_is_not_ready_to_pay"] if r["store_id"] == store]
    outputs, people = _outputs(night, store, day), _people(night, store)
    case.add(
        "caught", "tests", "Now with gates: gate_publish sees a store with no day",
        "Nothing is set aside: there is no row to set aside. The publish gate builds the list of "
        "days every open store should have, and holds each against the tills. For this store and "
        "day it finds no lines at all. The five other stores are published. This one is marked.",
        "Partial load", "night", _tasks(8), {"outputs": outputs, "people": people}, tone="caught",
        now="store_is_not_ready_to_pay",
        **case.caught("publish", "store_is_not_ready_to_pay", stored,
                      columns=["store_id", "sales_date", "problem"]),
    )  # fmt: skip
    _told(case, night, f"{store} {_day(day)}", "Runbook", {"outputs": outputs, "people": people},
          "The last line of the alert names a section of the runbook: a short note that says what "
          "the alert means and the first things to check.")  # fmt: skip
    case.facts = {"store": store, "day": day, "right": right, "manager": manager,
                  "outputs": outputs, "people": people, "later": later}  # fmt: skip
    return case


def case_8(nights: dict, key: dict) -> dict:
    """The late file: told on Thursday night, and resolved by itself on Friday night."""
    night, friday = nights[THU], nights[FRI]
    case = _file_case("8", night, friday, key, store="S06", day=THU,
                      name="One store's file arrives a day late",
                      hint="one store's file arrives a day late")  # fmt: skip
    store, day, right = "S06", THU, case.facts["right"]
    case.night, case.gates_run = friday, []
    case.gate_state = {"landing": "", "clean": "", "publish": ""}
    landing_gate = case.ran("landing")
    late = [r for r in landing_gate["stored"]["file_arrived_on_time"] if r["store_id"] == store]
    case.history = friday["history"]
    arrived = _card(f"sales_lines_{store}_{day}.csv", "", "arrived Sat 08-29, 00:40", "changed")
    case.add(
        "late", "tests", "One night later the file arrives, and a freshness test names it",
        "No fix is needed. The file comes in at 00:40, a day late, and Friday night's run loads "
        "it. A new test at the landing gate compares the moment a file arrived with the moment "
        "it was due: 01:00 the morning after its day. It is a warning. The rows are good, so "
        "they go on. Notice what it could not see on Thursday night: a file that is not there "
        "cannot be late.",
        "Freshness", f"{_day(FRI + timedelta(days=1))}, 02:00", _tasks(3),
        {"files": _stage("The file is here", arrived), "raw": _stage("Loaded tonight", arrived)},
        tone="warn", now="file_arrived_on_time", moves=[{"from": "files", "to": "raw"}],
        **case.caught("landing", "file_arrived_on_time", late,
                      columns=["file_name", "received_at", "due_at"]),
    )  # fmt: skip
    case.ran("clean")
    case.ran("publish")
    case.alerts = alert_cards(friday["alerts"], f"{store}")
    published = int(friday["sales"][(day.isoformat(), store)]["net_sales_cents"])
    outputs, people = _outputs(friday, store, day), _people(friday, store)
    case.add(
        "right", "alerts", f"The ticket closes by itself: RESOLVED, and {money(published)} is right",
        f"The day now adds up to the tills: {money(published)}, the same as the answer key, "
        f"{money(right)}. The problem of Thursday night is gone, so the alert is closed, and "
        f"{alerts.OPERATIONS} is told that too. The lateness itself goes to the digest as a "
        "message for store operations: nobody has to act tonight.",
        "Incident", f"{_day(FRI + timedelta(days=1))}, 02:00", _tasks(8),
        {"outputs": outputs, "people": people}, callback="publish",
    )  # fmt: skip
    sunday = nights[SUN]
    h = sunday["health"]
    _history_step(
        case, sunday, {"outputs": outputs, "people": people},
        f"\"On time\" is now a number. A sales file is due by 01:00. In the bonus week "
        f"{h['files_on_time']} of {h['files_expected']} made it: {h['files_on_time_percent']}%. "
        f"That measured number is an SLI. The target for it is {h['files_on_time_target']}%: an "
        "SLO. This week the target is missed, by one late file and one that never came. An SLA "
        "would be the same promise made to other people, with a cost when it is broken.",
        "SLI, SLO and SLA", f"{_day(SUN + timedelta(days=1))}, 08:00",
    )  # fmt: skip
    case.data["caught_by"] += ["told · " + alerts.OPERATIONS, "gate_landing · file arrived on time · warning",
                               "resolved · the next night, by itself"]  # fmt: skip
    case.data["recovered"] = {"published": money(published), "answer_key": money(right),
                              "equal": published == right}  # fmt: skip
    case.data["outcome"] = "Told on Thursday night. Resolved by itself on Friday night"
    return case.finish()


def case_7(nights: dict, recovered: dict, key: dict) -> dict:
    """The file that never comes: one ticket, sent once, open to the end."""
    night = nights[TUE]
    case = _file_case("7", night, nights[SUN], key, store="S04", day=TUE,
                      name="One store sends no file", hint="one store sends no file, ever")  # fmt: skip
    f = case.facts
    week = [nights[d]["alerts"] for d in WEEK]
    sent, shouted = sum(len(a["sent"]) for a in week), sum(a["shouted"] for a in week)
    still = [
        d
        for d in WEEK[2:]
        if any("S04 Tue" in a["title"] for a in nights[d]["alerts"]["still_open"])
    ]
    sunday = nights[SUN]
    case.alerts = alert_cards(sunday["alerts"], "S04")
    case.history = sunday["history"]
    shown = {"outputs": _outputs(sunday, "S04", TUE), "people": _people(sunday, "S04")}
    case.add(
        "fatigue", "alerts", "Five more nights, and the ticket is not sent again",
        f"The file does not come on Wednesday, or after. The problem is still true on "
        f"{_count(len(still), 'night')} more. Sending the same ticket every night would be easy, "
        f"and it would teach people to stop reading. That has a name: alert fatigue. In the whole "
        f"week this pipeline sent {sent} alerts. One alert for every failed test, every night, "
        f"would have been {shouted}. So a problem is sent once, when it is new. After that it is "
        "counted as still open.",
        "Alert fatigue", f"{_day(SUN + timedelta(days=1))}, 02:00", _tasks(8), shown,
        callback="publish",
    )  # fmt: skip
    _history_step(
        case, sunday, shown,
        "The run history says every night ran and published: 7 of 7. So the pipeline did its "
        "job, and a day of data is still missing. A job that ran is not the same as data that is "
        "right. Watching the data from outside, by its freshness, its volume, its shape, its "
        "values and what depends on it, is called observability.",
        "Observability", f"{_day(SUN + timedelta(days=1))}, 08:00",
    )  # fmt: skip
    after = recovered[5]["night"]
    case.alerts = {"counts": "After the recovery of the other stores: 1 ticket still open",
                   "cards": [{"level": "ticket", "to": t["to_whom"], "title": t["title"], "do": "",
                              "state": "still open"} for t in after["open_tickets"]
                             if "S04" in t["title"]]}  # fmt: skip
    people = _people(after, "S04")
    case.add(
        "monday", "people", "Monday: this is the one store that cannot be paid",
        f"The four other stores sent corrected files, and their nights were run again. S04's "
        f"Tuesday file never came. So the answer to the Monday question is: yes for five stores, "
        f"no for one, and here is the row that says why. {f['manager']['manager_name']}'s bonus "
        f"waits for {money(f['right'])} of sales that the tills counted and no file shows. "
        "The ticket stays open until the file arrives. That is not a fault of the pipeline. "
        "It is the pipeline telling the truth.",
        "Incident", f"{_day(SUN + timedelta(days=1))}, 09:00", _tasks(8),
        {"outputs": _outputs(after, "S04", TUE), "people": people}, tone="caught",
    )  # fmt: skip
    case.data["caught_by"] += ["told · " + alerts.OPERATIONS, "still open · the file never came"]
    case.data["outcome"] = "Told on Tuesday night. Still open on Monday: do not pay yet"
    return case.finish()


# ---------------------------------------------------------------- the four drills


def _drill_case(number: int, d: dict) -> dict:
    kind, store, first, second = d["kind"], d["store"], d["first"], d["second"]
    iso = DRILL_DAY.isoformat()
    right = d["right"]["net_sales_cents"]
    manager = next(s for s in first["report"]["stores"] if s["store_id"] == store)
    _columns, rows = d["sent_file"]
    status = first["status"][(iso, store)]
    published = first["sales"].get((iso, store), {}).get("net_sales_cents")
    test = {"file-twice": ("clean", "true_key_unique"), "rename-column": ("landing", "file_is_missing_a_column"),
            "late-file": ("publish", "store_is_not_ready_to_pay"), "bad-prices": ("clean", "impossible_price")}[kind]  # fmt: skip
    case = Case(
        f"D{number}", first, label=f"Drill {number}", store=store, hint=d["name"][0].lower() + d["name"][1:],
        name=d["name"], where=f"{_day(DRILL_DAY)}, a clean week · store {store}, {manager['store_name']}",
        table="sales_lines", output="daily_store_sales", lab=True,
        says_label="On the night of the fault", key_label="The answer key says",
        pipeline_says=money(int(published)) if published is not None else "no row at all",
        answer_key_says=money(right), table_is_right=published == right,
        caught_by=[f"{d['gate']} · {test[1].replace('_', ' ')}", f"told · {d['told']}",
                   f"fixed · {d['fixed']}"],
    )  # fmt: skip

    price_cards = [
        _line_card(r, "bad", f"unit price {money(int(r['unit_price_cents']))}") for r in rows[::10][:3]
    ] if kind == "bad-prices" else []  # fmt: skip
    plant = {
        "file-twice": (f"{store} sends its sales file, and twelve minutes later sends it again",
                       f"The second file has every line of the first, {len(rows)} of them, with new "
                       "line ids. So the standard unique check on line_id will pass.",
                       _stage(f"2 files from {store}",
                              _card(f"sales_lines_{store}_{iso}.csv", "", f"{len(rows)} lines"),
                              _card(f"sales_lines_{store}_{iso}_2.csv", "", f"the same {len(rows)} lines, again", "bad"))),
        "rename-column": (f"{store}'s export program gives a column a new name",
                          "The column amount_cents is now called amount. The loader reads a file by "
                          "column name, so it would fill amount_cents with nothing, for every line.",
                          _stage(f"1 file from {store}",
                                 _card(f"sales_lines_{store}_{iso}.csv", "", "amount_cents is missing. amount is new", "bad"))),
        "late-file": (f"01:00 passes, and {store} has sent no sales file",
                      "The file exists. It is only late: it will arrive at 00:40 the night after.",
                      _stage(f"5 files, none from {store}",
                             _card(f"sales_lines_{store}_{iso}.csv", "", "not here yet", "bad"))),
        "bad-prices": (f"{store} sends a file where every tenth line is 100 times too dear",
                       f"{len(rows[::10])} of the file's {len(rows)} lines carry a price 100 times "
                       "the price list. Each is a valid number.",
                       _stage(f"{len(rows)} lines from {store}",
                              *price_cards)),
    }[kind]  # fmt: skip
    case.add("plant", "files", plant[0], "A drill: a fault switched on in the stores on purpose, on "
             "a Thursday of a clean week. " + plant[1], "Incident", "evening",
             _tasks(0, "stores_send_files"), {"files": plant[2]}, plant=True)  # fmt: skip

    gate, name = test
    for g in ("landing", "clean", "publish"):
        found = case.ran(g)
        if g == gate:
            break
    stored = [
        r for r in found["stored"][name] if store in str(r.get("store_id", r.get("file_name", "")))
    ]
    aside_ids = {r["line_id"] for r in first["aside_lines"] if r["store_id"] == store}
    if first["refused"]:
        case.dead = _stage("1 file refused", _card(first["refused"][0]["file_name"], "",
                                                  first["refused"][0]["reason"], "bad"))  # fmt: skip
    if aside_ids:
        reasons = sorted({r["reason"] for r in first["aside_lines"] if r["store_id"] == store})
        case.aside = _stage(f"{_count(len(aside_ids), 'line')} set aside from {store}",
                            _card(f"{store} · {_day(DRILL_DAY)}", "", "; ".join(reasons), "bad"))  # fmt: skip
    what = {
        "file-twice": f"gate_clean sets {len(aside_ids)} extra copies aside. The first copy of every "
                      "line stays, so nothing is lost.",
        "rename-column": "gate_landing refuses the whole file. None of its rows go on, and an "
                         "empty amount never reaches a sum.",
        "late-file": "No row can be set aside. gate_publish marks the store's day: no sales file arrived.",
        "bad-prices": f"gate_clean sets the impossible prices aside. They are more than 5% of the "
                      f"file, so the rest is not trusted either: all {len(aside_ids)} lines are set aside.",
    }[kind]  # fmt: skip
    for g in ("landing", "clean", "publish"):
        if g not in case.gates_run:
            case.ran(g)
    outputs, people = _outputs(first, store, DRILL_DAY), _people(first, store)
    volume = next(
        (t for t in first["gates"]["clean"]["tests"] if t["name"] == "normal_volume"), None
    )
    if kind == "file-twice" and volume and volume["failed"]:
        what += (" A volume check noticed it too, as a warning: the store's day had about twice "
                 "its usual number of lines.")  # fmt: skip
    case.add(
        "caught", "tests", f"Which gate caught it: {d['gate']}", what,
        {"file-twice": "Volume", "rename-column": "Routing", "late-file": "Freshness",
         "bad-prices": "Alert"}[kind],
        "night", _tasks(8), {"outputs": outputs, "people": people}, tone="caught", now=name,
        **case.caught(gate, name, stored),
    )  # fmt: skip

    case.alerts = alert_cards(first["alerts"], store)
    case.history = first["history"]
    about = sorted(first["alerts"]["sent"], key=lambda a: a["to"] != alerts.OPERATIONS)
    ticket = next((a for a in about if store in a["title"]), None)
    saw = (f"{manager['manager_name']} saw the right number, {money(right)}, and \"yes\"."
           if status["status"] == "ok"
           else f"{manager['manager_name']} saw: DO NOT PAY YET. " + " ".join(manager["why_not"]) + ".")  # fmt: skip
    case.add(
        "told", "alerts", "What the manager saw, and who was told",
        saw + (f" A ticket went to {ticket['to']}: {ticket['title']}. It says what to do: "
               f"{ticket['do']}" if ticket else " Nobody has to act, so nobody is woken or "
               "interrupted. The news waits in the digest as a message. Not every problem needs "
               "a person."),
        "Callback" if ticket else "Alert fatigue", "night", _tasks(8),
        {"outputs": outputs, "people": people}, tone="caught" if ticket else "",
        callback="publish", **({"facts": _alert_facts(ticket)} if ticket else {}),
    )  # fmt: skip

    next_day = DRILL_DAY + timedelta(days=1)
    case.night, case.gates_run = second, []
    case.gate_state = {"landing": "", "clean": "", "publish": ""}
    for g in ("landing", "clean", "publish"):
        case.ran(g)
    case.history = second["history"]
    again_ids = {r["line_id"] for r in second["aside_lines"] if r["store_id"] == store}
    case.dead = dict(EMPTY) if not second["refused"] else case.dead
    case.aside = (_stage(f"{_count(len(again_ids), 'line')} set aside from {store}, again")
                  if again_ids else _stage(f"Nothing from {store} any more"))  # fmt: skip
    loaded = second["load"]["loaded_again"]
    how = {
        "file-twice": ("Nothing to fix. Run it again, and nothing changes",
                       "A store cannot take a file back, so the second copy stays in the landing "
                       f"folder. The night is run again: no file changed, so no file is loaded. The "
                       f"gate sets the same {len(again_ids)} copies aside again, and the day is right "
                       "again. A run that can be repeated with the same result is a safe re-run.",
                       "Safe re-run", "", next_day),
        "rename-column": ("The store sends the file again, and that one night is run again",
                          f"The corrected file has the same name. The load step sees a new "
                          f"fingerprint and loads it again: {', '.join(loaded)}. Loading and building "
                          "the same data again after the source was corrected is called reprocessing.",
                          "Reprocessing", BACKFILL.format(day=iso, after=next_day.isoformat()), next_day),
        "late-file": ("No fix. The next night loads the file by itself",
                      "The file arrives at 00:40, and the run of the next night loads it. The "
                      "freshness test at the landing gate names it: late, as a warning.",
                      "Freshness", "", next_day + timedelta(days=1)),
        "bad-prices": ("The store sends the real prices, and that one night is run again",
                       f"One command runs Thursday's night again: a backfill. The changed file is "
                       f"loaded again, and its old rows go first: {', '.join(loaded)}.",
                       "Backfill", BACKFILL.format(day=iso, after=next_day.isoformat()), next_day),
    }[kind]  # fmt: skip
    case.add("rerun", "raw", how[0], how[1], how[2], f"{_day(how[4])}, 09:30", _tasks(8),
             {"raw": _stage(f"{_count(len(loaded), 'changed file')} loaded again" if loaded
                            else "No file changed. Nothing is loaded again")},
             **({"sql": how[3]} if how[3] else {}))  # fmt: skip

    case.alerts = alert_cards(second["alerts"], store)
    after = int(second["sales"][(iso, store)]["net_sales_cents"])
    outputs, people = _outputs(second, store, DRILL_DAY), _people(second, store)
    closed = [
        r["title"]
        for r in second["alerts"]["resolved"]
        if store in r["title"] and r["level"] == "ticket"
    ]
    case.add(
        "right", "people", f"Right again: {money(after)}, the same as the answer key",
        f"{store}'s Thursday is {money(after)}. The answer key says {money(right)}. "
        + (f"The ticket is closed: RESOLVED, {closed[0]}. " if closed else "")
        + "The drill ends with an incident note: which gate caught it, what was set aside, what "
        "the manager saw, and what was changed.",
        "Incident note", f"{_day(how[4])}, 09:31", _tasks(8), {"outputs": outputs, "people": people},
        moves=[{"from": "outputs", "to": "people"}],
    )  # fmt: skip
    case.data["recovered"] = {"published": money(after), "answer_key": money(right),
                              "equal": after == right}  # fmt: skip
    case.data["outcome"] = f"{d['gate']} caught it. {d['fixed']}. Right again"
    return case.finish()


# ---------------------------------------------------------------- everything, in order


def all_cases(nights: dict, recovered: dict, drills: dict, landing: Path, temp: Path, world,
              key: dict) -> list[dict]:  # fmt: skip
    cases = [
        _recovered_case(
            "1", nights[MON], landing, key, recovered[1], store="S03", day=MON,
            name="A refund stored as a positive amount", hint="a refund stored as a positive amount",
            story="A customer brings a bottle of olive oil back to S03. The till gives $9.49 back. "
            "On the way out of the store the minus sign is lost: the file says +949.",
            tests=["refund_gives_money_back", "line_adds_up"], word="Stored failures",
            words=["Callback", "Root cause", "Backfill", "Incident note"],
        ),
        _recovered_case(
            "3", nights[THU], landing, key, recovered[3], store="S05", day=THU,
            name="A weight entered in grams, not kilograms", hint="a weight in grams, not kilograms",
            story="A customer at S05 buys 1.180 kg of bananas for $1.52. The export writes the "
            "scale's 1,180 grams as kilograms: one line of $1,522.20.",
            tests=["believable_quantity"], word="Severity",
            words=["Alert", "Root cause", "Reprocessing", "Incident note"],
        ),
        _recovered_case(
            "5", nights[SUN], landing, key, recovered[5], store="S02", day=SUN,
            name="A sale at 03:10 in a store that closes at 22:00", hint="a sale at 03:10, in a closed store",
            story="At 21:35 on Sunday the power goes at S02, and a till restarts with a wrong clock. "
            "The next receipt, $23.95, is stamped Monday 03:10: a day in the next bonus week.",
            tests=["inside_opening_hours"], word="Stored failures",
            words=["Routing", "Root cause", "Backfill", "Incident"],
        ),
        _recovered_case(
            "6", nights[SAT], landing, key, recovered[6], store="S06", day=SAT,
            name="A sale for a product that is not in products", hint="a product nobody typed yet",
            story="S06 already sells a new soup. The buying team has not typed it into the price "
            "list. The cleaning step joins every sale to a product, so three lines find no partner.",
            tests=["every_sale_has_a_product"], word="Severity",
            words=["Runbook", "Root cause", "Reprocessing", "Incident note"],
        ),
        case_8(nights, key),
        case_7(nights, recovered, key),
    ]  # fmt: skip
    cases += [_drill_case(n, d) for n, d in enumerate(drills.values(), start=1)]
    return cases


def block_case(temp: Path, base: Path, landing: Path) -> dict:
    """A store deleted from the stores table: the gate blocks, and this time someone is paged."""
    folder = temp / "lab_block"
    _copy(base, folder)
    _point_at(folder, landing)
    before = _rows("SELECT * FROM output.daily_store_sales")
    lines = _rows(
        "SELECT sum(amount_cents) AS cents FROM clean.sales_lines WHERE store_id = 'S06'"
    )[0]
    with connect() as wh:
        wh.execute("DELETE FROM raw.stores WHERE store_id = 'S06' "
                   "AND _file_name = (SELECT max(_file_name) FROM raw.stores)")  # fmt: skip
    night = _run_night(LAB_DAY, first="clean")
    assert night["blocked_at"] == "clean", "the lab case was meant to block at the clean gate"
    assert _rows("SELECT * FROM output.daily_store_sales") == before
    pages = night["alerts"]["sent"]
    case = Case(
        "B", night, lab=True, label="Planted in the lab", store="stores",
        hint="a store is deleted: the gate blocks, and pages", name="A store is deleted from the stores table",
        where="Four clean weeks · the stores table", table="stores", output="daily_store_sales",
        says_label="With no gate, S06 would lose", key_label="With the gate",
        pipeline_says=money(int(lines["cents"])), answer_key_says="nothing is published",
        caught_by=["gate_clean · six stores once · block", f"told · {pages[0]['to']} · page"],
    )  # fmt: skip
    stores = _rows("SELECT store_id, store_name FROM clean.stores")
    stage = _stage(f"{len(stores)} stores, not 6", _card("S06: gone", "", "the row was deleted by mistake", "bad"),
                   *[_card(r["store_id"], "", r["store_name"]) for r in stores[:3]])  # fmt: skip
    case.add(
        "plant", "files", "Somebody deletes a row from the stores table",
        "This fault is planted in the lab, on a private copy of four clean weeks. The stores "
        "table has six rows, and every sale is joined to it. One row is gone.",
        "Incident", "evening", _tasks(0), {"files": stage}, plant=True,
    )  # fmt: skip
    case.ran("clean")
    stored = night["gates"]["clean"]["stored"]["six_stores_once"]
    total = sum(int(r["net_sales_cents"]) for r in before)
    outputs = {"count": "daily_store_sales: the last good numbers", "staging": "nothing new is published",
               "cards": [_card(f"{len(before)} rows, as last night", money(total), "not one row changed", "good")]}  # fmt: skip
    tasks = _tasks(TASKS.index("gate_clean"), failed="gate_clean")
    case.add(
        "blocked", "tests", 'gate_clean: the test "six stores once" fails, and the gate BLOCKS',
        "Milestone 4 built this part. The command ends with exit code 1, Airflow marks gate_clean "
        "as failed, and build, gate_publish and publish get the state upstream_failed. Nothing new "
        f"is published: {len(before)} rows, {money(total)}, exactly as last night.",
        "Upstream failure", "night", tasks, {"clean": stage, "outputs": outputs}, tone="blocked",
        now="six_stores_once", **case.caught("clean", "six_stores_once", stored),
    )  # fmt: skip
    case.alerts = alert_cards(night["alerts"])
    case.history = night["history"]
    people = _stage("Someone is awake", _card(pages[0]["to"], "", "paged at 02:00", "changed"),
                    _card("Head of retail operations", "", "reads last night's numbers, which were right", "good"))  # fmt: skip
    case.add(
        "paged", "alerts", f"The failure callback: {_count(len(pages), 'page')} to {pages[0]['to'].lower()}",
        "This is new. Every task has a failure callback. When gate_clean fails, Airflow calls it, "
        "and the same alerts command runs with the name of the failed task. A blocked gate is the "
        "most urgent thing this pipeline can say, so its level is page: wake someone. "
        f"{_count(len(pages), 'test')} saw the missing store, so {len(pages)} pages went out. "
        "A ticket can wait for the morning. Six stores with no new numbers cannot.",
        "Callback", "night", tasks, {"outputs": outputs, "people": people}, tone="blocked",
        callback="gate_clean", facts=_alert_facts(pages[0]),
    )  # fmt: skip
    blocked = next(h for h in night["history"] if h["blocked_at"])
    case.add(
        "history", "history", "The run history keeps the blocked night",
        f"The pipeline writes one row for every step of every run. For {blocked['day']} it now "
        f"says: blocked at {blocked['blocked_at']}. So in the morning anyone can see which night "
        "did not publish, and where it stopped, without asking the person who was paged.",
        "Run history", "morning", tasks, {"outputs": outputs, "people": people},
    )  # fmt: skip
    case.data["outcome"] = (
        "gate_clean blocks, the outputs keep their last good numbers, and someone is paged"
    )
    return case.finish()


def monday_answer(nights: dict, recovered: dict, world) -> dict:
    """The end of the project: the report before the recovery, and after it."""
    last, after = nights[SUN], recovered[5]["night"]
    right = truth.week_net_sales(world, BONUS_WEEK_START)

    def report_stage(night: dict) -> dict:
        ready = sum(s["pay"] for s in night["report"]["stores"])
        return _stage(
            f"{ready} of 6 stores can be paid",
            *[_card(f"{s['store_id']} · {s['manager_name']}", money(s["net_sales_cents"]),
                    "Pay? yes · equal to the answer key" if s["pay"] and s["net_sales_cents"] == right[s["store_id"]]
                    else "Pay? yes" if s["pay"] else "DO NOT PAY YET. " + " ".join(s["why_not"]),
                    "good" if s["pay"] else "bad") for s in night["report"]["stores"]],
        )  # fmt: skip

    ready_before = sum(s["pay"] for s in last["report"]["stores"])
    ready_after = sum(s["pay"] for s in after["report"]["stores"])
    case = Case(
        "M", last, label="The Monday answer", store="all six stores",
        hint="the report before the recovery, and after", name="Can I pay the bonus, store by store?",
        where=f"{_day(SUN + timedelta(days=1))} · the head of retail operations signs off the bonus",
        table="sales_lines", output="daily_store_sales", says_label="Before the recovery",
        key_label="After it", pipeline_says=f"{ready_before} of 6 can be paid",
        answer_key_says=f"{ready_after} of 6 can be paid", table_is_right=True,
        caught_by=["yes for five stores", "no for one", "and the row that says why"],
    )  # fmt: skip
    for gate in ("landing", "clean", "publish"):
        case.ran(gate)
    week = [nights[d]["alerts"] for d in WEEK]
    sent, shouted = sum(len(a["sent"]) for a in week), sum(a["shouted"] for a in week)
    case.alerts = alert_cards(last["alerts"])
    case.history = last["history"]
    before_stage = report_stage(last)
    case.add(
        "before", "people", f"Sunday night: {ready_before} of 6 stores can be paid",
        "This is where milestone 4 ended. The gates did their work all week: bad rows were set "
        f"aside, and the report says for each store whether to pay. {6 - ready_before} stores "
        "wait, each with one line that names the day and the reason. In milestone 4 nobody knew "
        "until Monday.",
        "Incident", f"{_day(SUN + timedelta(days=1))}, 02:00", _tasks(8), {"people": before_stage},
        tone="caught", plant=True,
    )  # fmt: skip
    case.add(
        "alerts", "alerts", f"All week, the right person was told on the night: {sent} alerts, not {shouted}",
        f"Each problem went to the person who can act on it, once, when it was new: store days to "
        f"{alerts.OPERATIONS.lower()}, refused supplier files to finance and the buying team, a "
        "missing count to the replenishment team. That is routing. One alert for every failed "
        f"test on every night would have been {shouted}, and nobody reads {shouted} alerts.",
        "Routing", f"{_day(SUN + timedelta(days=1))}, 02:00", _tasks(8), {"people": before_stage},
        callback="publish",
    )  # fmt: skip
    h = last["health"]
    _history_step(
        case, last, {"people": before_stage},
        f"Did it run? {h['nights_published']} of {h['nights']} nights. Is the data fresh? "
        f"{h['files_on_time']} of {h['files_expected']} files by 01:00, under the target of "
        f"{h['files_on_time_target']}%. Did every store report? {h['store_days_ok']} of "
        f"{h['store_days']} store days can be paid from. Five signals are watched across this "
        "project: freshness and volume here, the schema of every file at the landing gate, the "
        "spread of values in the range and price checks, and lineage in the map of who reads what.",
        "Observability", f"{_day(SUN + timedelta(days=1))}, 08:00",
    )  # fmt: skip
    nights_again = ", ".join(_day(r["day"]) for r in recovered.values())
    case.night, case.gates_run = after, []
    case.gate_state = {"landing": "", "clean": "", "publish": ""}
    for gate in ("landing", "clean", "publish"):
        case.ran(gate)
    case.history = after["history"]
    case.alerts = alert_cards(after["alerts"])
    case.add(
        "recover", "raw", "Four stores send it right, and four nights are run again",
        f"Because they were told in time, four stores fixed their fault at the source. Only the "
        f"nights that were wrong are run again: {nights_again}. Each one is a backfill of one "
        "night. The load step takes the changed files, and nothing else is loaded twice.",
        "Backfill", f"{_day(SUN + timedelta(days=1))}, 09:30", _tasks(8),
        {"raw": _stage("4 stores, 4 nights", *[_card(f"Case {n}", "", r["fix"], "good")
                                               for n, r in list(recovered.items())[:4]])},
    )  # fmt: skip
    after_stage = report_stage(after)
    paid = [s for s in after["report"]["stores"] if s["pay"]]
    equal = sum(s["net_sales_cents"] == right[s["store_id"]] for s in paid)
    case.add(
        "after", "people", f"The Monday answer: yes for {ready_after} stores, no for one",
        f"{ready_after} stores can be paid, and for {equal} of them the published week equals the "
        "answer key to the cent. One store cannot: S04. Its row says why: "
        + " ".join(next(s for s in after["report"]["stores"] if not s["pay"])["why_not"])
        + ". That is the whole second sentence of her question: which one, and why.",
        "Safe re-run", f"{_day(SUN + timedelta(days=1))}, 10:00", _tasks(8), {"people": after_stage},
        moves=[{"from": "outputs", "to": "people"}],
    )  # fmt: skip
    case.add(
        "done", "people", "Nothing is still wrong",
        "A wrong row is stopped at a gate. A person is told on the night, in words they can act "
        "on. A fixed day comes back right with one command, and the numbers equal the answer "
        "key. What is left open is honest: one store's file never came, and the report says so.",
        "Incident note", f"{_day(SUN + timedelta(days=1))}, 10:00", _tasks(8), {"people": after_stage},
    )  # fmt: skip
    case.data["recovered"] = {"published": f"{ready_after} of 6", "answer_key": f"{equal} equal the answer key",
                              "equal": equal == len(paid)}  # fmt: skip
    case.data["outcome"] = f"Yes for {ready_after} stores, no for one, and the row that says why"
    return case.finish()
