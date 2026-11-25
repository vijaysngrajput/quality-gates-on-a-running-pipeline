"""The cases of the milestone 4 flow page that are not a bad row in a store's sales file.

flow_data.py runs the nights and builds the sales cases. This file builds the others from
the same nights: a warning, a missing file, two supplier files refused at the door, a missing
count, one person twice, and two faults planted in the lab that BLOCK.
"""

from datetime import date
from pathlib import Path

from lotus_pipeline import contract
from lotus_pipeline.flow_data import (
    EMPTY,
    FRI,
    LAB_DAY,
    MON,
    SAT,
    SUN,
    TASKS,
    THU,
    TUE,
    WED,
    Case,
    _card,
    _copy,
    _count,
    _day,
    _line_card,
    _mask,
    _outputs,
    _people,
    _point_at,
    _qty,
    _read_csv,
    _rows,
    _run_night,
    _sales_case,
    _table,
    _tasks,
)
from lotus_pipeline.report import money
from lotus_pipeline.warehouse import connect
from store_systems import truth
from store_systems.catalog import HOLIDAY, PRODUCTS

CONTRACT_FILE = "contracts/supplier_prices.yml"


def _landing_step(case: Case, stages: dict, extra: str = "") -> dict:
    found = case.ran("landing")
    refused = found["refused"]
    if refused:
        case.dead = {"count": _count(len(refused), "file") + " refused",
                     "cards": [_card(f, "", "refused at the door", "bad") for f in refused]}  # fmt: skip
    case.add(
        "landing", "raw", "Now the same night, with gates. The first one stands where the files land",
        f"Airflow loads the files, then starts gate_landing. It runs {len(found['tests'])} tests on "
        "the raw tables and on the files' own columns. " + extra,
        "Gate placement", "night", _tasks(3), stages, moves=[{"from": "files", "to": "raw"}],
    )  # fmt: skip
    return found


# ---------------------------------------------------------------- case 4: a warning


def case_4(night: dict, landing: Path, key: dict) -> dict:
    store, day = "S04", FRI
    iso = day.isoformat()
    right = key[(day, store)]["net_sales_cents"]
    stored = [
        r for r in night["gates"]["clean"]["stored"]["price_of_the_day"] if r["store_id"] == store
    ]
    short = sum(int(r["store_short_cents"]) for r in stored)
    packs = sum(float(r["quantity"]) for r in stored)
    _c, sent = _read_csv(landing, "sales_lines", f"sales_lines_{store}_{iso}.csv")
    ids = {r["line_id"] for r in stored}
    butter = [_line_card(r, "changed", f"charged {money(int(r['unit_price_cents']))} a pack")
              for r in sent if r["line_id"] in ids][:3]  # fmt: skip
    good = [_line_card(r) for r in sent if r["line_id"] not in ids][:3]
    manager = next(s for s in night["report"]["stores"] if s["store_id"] == store)
    published = int(night["sales"][(iso, store)]["net_sales_cents"])
    case = Case(
        "4", night, label="Case 4", store=store, hint="yesterday's price, charged today",
        name="A price that was changed yesterday, charged at the old price",
        where=f"{_day(day)} · store {store}, {manager['store_name']}", table="sales_lines",
        output="daily_store_sales", says_label="The report says", key_label="The answer key says",
        pipeline_says=money(published), answer_key_says=money(right), table_is_right=True,
        note=f"The table is right. The store is {money(short)} short.",
        caught_by=["gate_clean · price of the day · warning"],
        outcome=f"A warning on {len(stored)} lines. The store is {money(short)} short, and the day can be paid",
    )  # fmt: skip
    files_stage = {"count": f"{len(sent)} lines from {store}", "cards": butter + good}
    case.add(
        "plant", "files", "What the store sent",
        f"Butter costs $3.99 from today. The tills of {store} still have yesterday's price, $3.49. "
        f"They sell {_qty(packs)} packs at it. Every line is a valid line.",
        "Gate", "evening", _tasks(0, "stores_send_files"), {"files": files_stage}, plant=True,
    )  # fmt: skip
    case.add(
        "before", "people", "First, the old run: five green checks, a right number, and money lost",
        f"The report says {money(published)}, and that is what the tills took. So the number is "
        f"right. But the store took {money(short)} less than the price list says, and finance "
        "was never told. A check can protect money that no table shows.",
        "Generic test", "morning", _tasks(0),
        {"files": files_stage, "people": {"count": "Finance", "cards": [
            _card("Finance", money(short), "lost at the till. Nobody knows", "bad")]}},
        tone="problem",
    )  # fmt: skip
    _landing_step(case, {"files": files_stage, "raw": files_stage},
                  "Nothing is wrong with the file, so it goes on.")  # fmt: skip
    clean_stage = {"count": f"{len(sent)} lines from {store}", "cards": butter + good}
    case.ran("clean")
    case.add(
        "caught", "tests", 'gate_clean: the test "price of the day" fails, with severity warn',
        f"The test joins each line to the price valid on its day. {len(stored)} lines of {store} "
        "were charged another price. Read what the file tells dbt: severity warn, action warn. "
        "The rows are true. The till really took that money. So they are not set aside.",
        "Severity", "night", _tasks(4, "gate_clean"), {"clean": clean_stage}, tone="warn",
        now="price_of_the_day", moves=[{"from": "raw", "to": "clean"}],
        **case.caught("clean", "price_of_the_day", stored,
                      columns=["line_id", "product_name", "quantity", "charged_cents",
                               "price_list_cents", "store_short_cents"]),
    )  # fmt: skip
    case.add(
        "warn", "clean", "The task acts: a warning is recorded, and the rows walk on",
        "For each failed check somebody chose: block, set aside, or warn. Here the sales number "
        "is right, so stopping it would hurt the manager for nothing. The warning is written to "
        "the gate's results, with the six lines. Finance can read it there.",
        "Warn or block", "night", _tasks(5), {"clean": clean_stage}, tone="warn",
    )  # fmt: skip
    case.ran("publish")
    outputs = _outputs(night, store, day, staged=True)
    case.add(
        "audit", "outputs", "Write, audit, publish: the day adds up to the tills",
        f"The lines of {store} add up to {money(published)}, and so do its tills. gate_publish "
        "has nothing to say about this day.",
        "Write, audit, publish", "night", _tasks(6, "gate_publish"),
        {"clean": clean_stage, "outputs": outputs}, moves=[{"from": "clean", "to": "outputs"}],
    )  # fmt: skip
    people, outputs = _people(night, store), _outputs(night, store, day)
    held = (
        ""
        if manager["pay"]
        else (
            f" {store} is held back this week for another reason: "
            + " ".join(manager["why_not"])
            + "."
        )
    )
    case.add(
        "people", "people", "Friday can be paid from. The warning is for finance",
        f"The price warning does not hold back anybody's pay.{held}",
        "Rejected rows", "morning", _tasks(8), {"outputs": outputs, "people": people},
        moves=[{"from": "outputs", "to": "people"}],
    )  # fmt: skip
    return case.finish(people, outputs)


# ---------------------------------------------------------------- case 7: no file


def case_7(night: dict, landing: Path, key: dict) -> dict:
    store, day = "S04", TUE
    iso = day.isoformat()
    right = key[(day, store)]["net_sales_cents"]
    manager = next(s for s in night["report"]["stores"] if s["store_id"] == store)
    _c, tills = _read_csv(landing, "till_totals", f"till_totals_{store}_{iso}.csv")
    till = sum(int(t["net_total_cents"]) for t in tills)
    status = night["status"][(iso, store)]
    stored = [r for r in night["gates"]["publish"]["stored"]["store_is_not_ready_to_pay"]
              if r["store_id"] == store and r["sales_date"] == iso]  # fmt: skip
    others = [s for s in night["report"]["stores"] if s["store_id"] != store]
    case = Case(
        "7", night, label="Case 7", store=store, hint="one store sends no file",
        name="One store sends no file",
        where=f"{_day(day)} · store {store}, {manager['store_name']}", table="sales_lines",
        output="daily_store_sales", says_label="Before the gates, the report said",
        key_label="The answer key says", pipeline_says="no row at all",
        answer_key_says=money(right),
        caught_by=["gate_publish · store is not ready to pay · warning", "the model store_day_status"],
        outcome=f"Nothing to set aside. The status says: no sales file arrived. {store}: do not pay yet",
    )  # fmt: skip
    files_stage = {"count": "5 of 6 stores sent their sales", "cards": [
        _card(f"{store}: no sales file", "", "the back-office computer was down", "bad"),
        _card(f"{store}: till totals", money(till), "the tills did send their own total", "good"),
    ]}  # fmt: skip
    case.add(
        "plant", "files", "What the store did not send",
        f"Five stores send their sales file. {store} sends none. Its tills still send their own "
        f"total: {money(till)} for the day.",
        "Gate", "evening", _tasks(0, "stores_send_files"), {"files": files_stage}, plant=True,
    )  # fmt: skip
    case.add(
        "before", "people", "First, the old run: five green checks, and a whole day is gone",
        f"There are no rows to fail a check, so all five pass. {store} has no row for {_day(day)} "
        f"in the report. {manager['manager_name']} loses the bonus on {money(right)}, and "
        "nothing says so.",
        "Generic test", "morning", _tasks(0),
        {"files": files_stage, "people": {"count": "The old report", "cards": [
            _card(f"{store} · {manager['manager_name']}", "–", f"no row for {_day(day)}. Checks: 5 of 5 passed", "bad")]}},
        tone="problem",
    )  # fmt: skip
    _landing_step(case, {"files": files_stage, "raw": {"count": f"0 lines from {store}", "cards": []}},
                  "A file that did not come cannot be refused. So this gate cannot see the fault."
                  + (" It did refuse another file tonight: that is case 10." if night["gates"]["landing"]["refused"] else ""))  # fmt: skip
    case.ran("clean")
    case.add(
        "clean", "tests", "gate_clean: no test fails for this store",
        f"dbt runs {len(night['gates']['clean']['tests'])} tests on the clean tables. A test "
        f"returns bad rows, and {store} has no rows. This gate cannot see the fault either. "
        "Where a gate stands decides what it can see.",
        "Gate placement", "night", _tasks(5), {"clean": {"count": f"0 lines from {store}", "cards": []}},
    )  # fmt: skip
    case.ran("publish")
    outputs = _outputs(night, store, day, staged=True)
    case.add(
        "caught", "tests", "gate_publish: the model store_day_status has a row for a day with no sales",
        "Before this gate, dbt builds a small model: one row for every store that was open, with "
        f"its lines and its tills side by side. For {store} it says: lines $0.00, tills "
        f"{money(int(status['till_cents']))}, problem no_sales_file. The model's SQL has a unit "
        "test of its own, with five stores typed by hand, one for each answer it can give.",
        "Unit test", "night", _tasks(6, "gate_publish"), {"outputs": outputs}, tone="caught",
        now="store_is_not_ready_to_pay",
        **case.caught("publish", "store_is_not_ready_to_pay", stored,
                      columns=["store_id", "sales_date", "problem", "rows_set_aside"]),
    )  # fmt: skip
    people, outputs = _people(night, store), _outputs(night, store, day)
    ready = sum(s["pay"] for s in others)
    case.add(
        "people", "people", "A partial load: five stores go on, and the report says what is missing",
        f"The gate does not block. Five stores did nothing wrong, and {ready} of them can be paid "
        f"tonight. {store} is marked: {manager['why_not'][0]}. The tills' total is there to show "
        "how much is missing.",
        "Partial load", "morning", _tasks(8), {"outputs": outputs, "people": people}, tone="caught",
        moves=[{"from": "outputs", "to": "people"}],
    )  # fmt: skip
    return case.finish(people, outputs)


# ---------------------------------------------------------------- cases 10 and 11: a file refused


def _supplier_case(cid: str, night: dict, landing: Path, world, *, supplier: str, day: date,
                   name: str, hint: str, story: str, old: str) -> dict:  # fmt: skip
    file_name = f"supplier_prices_{supplier}_{day.isoformat()}.csv"
    columns, sent = _read_csv(landing, "supplier_prices", file_name)
    problems = contract.check_file(landing / "supplier_prices" / file_name)
    refused = next(r for r in night["refused"] if r["file_name"] == file_name)
    costs = truth.true_costs(world, day)
    products = [r["product_id"] for r in sent][:3]
    cost_column = next(
        c for c in columns if c not in ("supplier_id", "product_id", "unit", "price_date")
    )
    file_cards = [_card(PRODUCTS[r["product_id"]][0], r[cost_column], f"{cost_column} · as the file has it", "bad")
                  for r in sent[:3]]  # fmt: skip
    files_stage = {
        "count": f"{len(sent)} rows from {supplier}. Columns: {', '.join(columns)}",
        "cards": file_cards,
    }
    orders = [night["orders"][("S03", p)] for p in products]
    order_cards = [_card(PRODUCTS[o["product_id"]][0], money(int(o["unit_cost_cents"])),
                         "the last good cost, on tomorrow's order list", "good") for o in orders]  # fmt: skip
    right = all(int(o["unit_cost_cents"]) == int(costs[o["product_id"]] * 100) for o in orders)
    try:  # how many times too high the file's first cost is, when it is a number at all
        times = round(float(sent[0][cost_column]) / float(costs[sent[0]["product_id"]]))
    except ValueError:
        times = 0
    old = old.format(n=len(sent), times=times)
    by_dbt = "file_is_missing_a_column" in night["gates"]["landing"]["stored"] and any(
        r["file_name"] == file_name
        for r in night["gates"]["landing"]["stored"]["file_is_missing_a_column"]
    )
    case = Case(
        cid, night, label=f"Case {cid}", store=supplier, hint=hint, name=name,
        where=f"{_day(day)} · the price file of supplier {supplier}", table="supplier_prices",
        output="replenishment_list", says_label="Before the gates", key_label="With the gates",
        pipeline_says=old, answer_key_says="the last good cost, on every order",
        caught_by=(["gate_landing · file is missing a column · file refused"] if by_dbt else [])
        + ["gate_landing · the supplier's contract · file refused"],
        outcome=f"The whole file is refused at the door. The last good cost is used for {len(sent)} products",
    )  # fmt: skip
    case.add("plant", "files", "What the supplier sent", story, "Gate", "evening",
             _tasks(0, "stores_send_files"), {"files": files_stage}, plant=True)  # fmt: skip
    case.add(
        "before", "people", "First, the old run: the load runs, and five green checks",
        f"The five standard checks only look at the sales. They pass. {old[0].upper() + old[1:]}. "
        "Finance pays suppliers from these costs.",
        "Generic test", "morning", _tasks(0),
        {"files": files_stage, "people": {"count": "Finance", "cards": [
            _card("Finance", "", old, "bad")]}}, tone="problem",
    )  # fmt: skip
    found = case.ran("landing")
    raw_stage = {"count": f"{len(sent)} rows from {supplier}, as received", "cards": file_cards}
    if by_dbt:
        stored = [
            r for r in found["stored"]["file_is_missing_a_column"] if r["file_name"] == file_name
        ]
        detail = case.caught("landing", "file_is_missing_a_column", stored)
        title = 'gate_landing: the test "file is missing a column" fails'
        text = (
            "The load step keeps the first line of every file. This test puts the columns we "
            f"expect beside the columns the file had. {stored[0]['missing_column']} is missing. "
            "A gate that stands this early is the only one that can still see a file's own columns."
        )
        now = "file_is_missing_a_column"
    else:
        rule = (
            (Path(contract.CONTRACTS) / "supplier_prices.yml")
            .read_text()
            .split("  - name: unit_cost")[1]
            .split("  - name:")[0]
        )
        detail = {
            "facts": [["The contract holds the rule", CONTRACT_FILE],
                      ["It is checked", "in Python, before dbt: lotus_pipeline/contract.py"],
                      ["Airflow runs it", "task gate_landing"],
                      ["It protects", "Finance: they pay suppliers from this file"]],
            "sql": "  - name: unit_cost" + rule.rstrip(),
            "rows": _table([{"line": p["row"], "column": p["column"], "value": p["value"], "problem": p["problem"]}
                            for p in problems if p["kind"] == "value"],
                           "What the contract says about this file"),
        }  # fmt: skip
        title = "gate_landing: the file breaks the supplier's contract"
        text = (
            "Every column is there, and every value is a number. A test on the columns passes. "
            "But the gate also holds each supplier file against the written contract. The cost "
            f"must be dollars with two decimals. {len([p for p in problems if p['kind'] == 'value'])} "
            "values have none. This one rule does not live in dbt: it is the contract, and it "
            "speaks at the door."
        )
        now = "supplier_contract"
    case.add("caught", "tests", title, text, "Gate placement", "night", _tasks(2, "gate_landing"),
             {"files": files_stage, "raw": raw_stage}, tone="caught", now=now,
             moves=[{"from": "files", "to": "raw"}], **detail)  # fmt: skip
    case.dead = {"count": _count(len(night["refused"]), "file") + " refused",
                 "cards": [_card(r["file_name"], "", r["reason"], "bad") for r in night["refused"]]}  # fmt: skip
    case.add(
        "refused", "dead", "The task acts: the whole file is refused",
        f"The file goes to the dead-letter table, with the reason: {refused['reason']}. None of "
        "its rows go on. Its rows stay in the raw table, so nothing is lost and a person can "
        "read them.",
        "Dead-letter table", "night", _tasks(3), {"raw": raw_stage}, tone="caught",
        moves=[{"from": "raw", "to": "dead"}],
    )  # fmt: skip
    case.ran("clean")
    good = _rows(
        "SELECT product_id, unit_cost_cents, price_date FROM clean.supplier_prices "
        "WHERE supplier_id = ? AND price_date = (SELECT max(price_date) FROM clean.supplier_prices "
        "WHERE supplier_id = ? AND price_date < CAST(? AS DATE))", (supplier, supplier, day),
    )  # fmt: skip
    good = sorted(good, key=lambda r: r["product_id"])[:3]
    clean_stage = {"count": f"The last good file of {supplier} stays in use", "cards": [
        _card(PRODUCTS[r["product_id"]][0], money(int(r["unit_cost_cents"])), f"from the file of {r['price_date']}", "good")
        for r in good]}  # fmt: skip
    case.add(
        "clean", "clean", "Cleaning leaves the refused file out",
        f"The clean step skips every file in the dead-letter table. For {supplier} the newest "
        "cost is now the one from last week's file. gate_clean runs its tests on that, and "
        "finds nothing to set aside.",
        "Rejected rows", "night", _tasks(5), {"clean": clean_stage},
    )  # fmt: skip
    case.ran("publish")
    outputs = {
        "count": "replenishment_list, for tomorrow",
        "staging": "published",
        "cards": order_cards,
    }
    people = {"count": "Finance, and the replenishment team", "cards": [
        _card("The order list", "", "every order has a cost" + (", and it is the true one" if right else ""), "good")]}  # fmt: skip
    case.add(
        "people", "people", "The order list has a cost on every line, and it is last week's",
        f"gate_publish checks that no order is without a cost. It passes. Finance can approve "
        f"tomorrow's order. The costs of {supplier} are a week old, which is right until the "
        "supplier sends a good file.",
        "Write, audit, publish", "morning", _tasks(8), {"outputs": outputs, "people": people},
        moves=[{"from": "clean", "to": "outputs"}],
    )  # fmt: skip
    return case.finish(people, outputs)


# ---------------------------------------------------------------- case 12: a missing count


def case_12(night: dict, landing: Path, world) -> dict:
    store, product, day = "S05", "P-001", FRI
    order = night["orders"][(store, product)]
    right = truth.replenishment_list(world, day)[(store, product)]
    sold, old_order = float(order["sold_yesterday"]), 2 * float(order["sold_yesterday"])
    _c, counted = _read_csv(landing, "stock_counts", f"stock_counts_{store}_{day.isoformat()}.csv")
    name = PRODUCTS[product][0]
    cards = [_card(f"{name}: no row", "", "nobody counted it", "bad")] + [
        _card(PRODUCTS[r["product_id"]][0], _qty(r["counted_qty"]), f"counted by {r['counted_by']}") for r in counted[:3]]  # fmt: skip
    files_stage = {"count": f"{len(counted)} counts from {store}", "cards": cards}
    stored = night["gates"]["clean"]["stored"]["a_count_for_every_day"]
    stored_order = night["gates"]["publish"]["stored"]["order_without_a_count"]
    case = Case(
        "12", night, label="Case 12", store=store, hint="a stock count is missing for one day",
        name="A stock count is missing for one day",
        where=f"{_day(day)} · store {store}, {name}", table="stock_counts",
        output="replenishment_list", says_label="Before the gates, the order was",
        key_label="The right order is", pipeline_says=f"{_qty(old_order)} bottles",
        answer_key_says=f"{_qty(right['order_qty'])} bottles",
        caught_by=["gate_clean · a count for every day · warning",
                   "gate_publish · order without a count · warning"],
        outcome=f"A warning. The order list says \"{order['note']}\" where it said {_qty(old_order)}",
    )  # fmt: skip
    case.add(
        "plant", "files", "What the store sent",
        f"{store} sold {_qty(sold)} bottles of {name} today. At closing time somebody walks the "
        f"store and counts the shelves. The milk is skipped. {_qty(right['stock_counted'])} "
        "bottles stand on that shelf.",
        "Gate", "evening", _tasks(0, "stores_send_files"), {"files": files_stage}, plant=True,
    )  # fmt: skip
    case.add(
        "before", "people", "First, the old run: the gap is read as an empty shelf",
        f"Lotus orders twice what sold, less what is on the shelf. With no count the old pipeline "
        f"read zero, and ordered {_qty(old_order)}. The right order is {_qty(right['order_qty'])}. "
        "Five green checks.",
        "Generic test", "morning", _tasks(0),
        {"files": files_stage, "people": {"count": "The replenishment team", "cards": [
            _card(f"{name} · {store}", f"order {_qty(old_order)}", f"the right order is {_qty(right['order_qty'])}", "bad")]}},
        tone="problem",
    )  # fmt: skip
    _landing_step(case, {"files": files_stage, "raw": files_stage},
                  "The file has every column. A row that is missing is not a column that is missing.")  # fmt: skip
    case.ran("clean")
    case.add(
        "caught", "tests", 'gate_clean: the test "a count for every day" fails, as a warning',
        "The test first builds the counts a day should have: every open store, every product on "
        f"sale. Then it looks for the ones that are not there. It returns {_count(len(stored), 'row')}. "
        "There is no bad row to set aside: the row does not exist. So the action is warn.",
        "Warn or block", "night", _tasks(4, "gate_clean"), {"clean": files_stage}, tone="warn",
        now="a_count_for_every_day", moves=[{"from": "raw", "to": "clean"}],
        **case.caught("clean", "a_count_for_every_day", stored,
                      columns=["store_id", "sales_date", "product_id", "product_name", "problem"]),
    )  # fmt: skip
    case.ran("publish")
    outputs = {"count": "replenishment_list, for tomorrow", "staging": "staging: written, not yet published",
               "cards": [_card(f"{name} · {store}", "no order", order["note"], "changed")]}  # fmt: skip
    case.add(
        "audit", "tests", 'gate_publish: "order without a count" marks the line',
        "A warning alone would still let the old order of 18 through. So the build step changed "
        "too: a product with no count gets no order, and a note. gate_publish then finds that "
        "line in staging and warns, so the result of the night names the product.",
        "Write, audit, publish", "night", _tasks(6, "gate_publish"), {"outputs": outputs},
        tone="warn", now="order_without_a_count",
        **case.caught("publish", "order_without_a_count", stored_order),
    )  # fmt: skip
    outputs = {**outputs, "staging": "published"}
    people = {"count": "The replenishment team", "cards": [
        _card(f"{name} · {store}", "by hand", f"{order['note']}. They count {_qty(right['stock_counted'])}, and order {_qty(right['order_qty'])}", "good")]}  # fmt: skip
    case.add(
        "people", "people", "The order list says what it does not know",
        f"The list no longer says {_qty(old_order)}. It says: {order['note']}. A person looks at "
        f"the shelf, finds {_qty(right['stock_counted'])} bottles, and orders "
        f"{_qty(right['order_qty'])}. Every other line of the list is as before.",
        "Partial load", "morning", _tasks(8), {"outputs": outputs, "people": people},
        moves=[{"from": "outputs", "to": "people"}],
    )  # fmt: skip
    return case.finish(people, outputs)


# ---------------------------------------------------------------- case 13: one person twice


def case_13(night: dict, landing: Path) -> dict:
    day = WED
    stored = night["gates"]["clean"]["stored"]["one_person_once"]
    newer, first = stored[0]["thing"], stored[0]["first_member_id"]
    _c, members = _read_csv(landing, "members", f"members_{day.isoformat()}.csv")
    emails = {
        m["member_id"]: _mask(m["email"]) for m in members if m["member_id"] in (newer, first)
    }
    cards = [_card(first, "", f"{emails[first]} · joined earlier"),
             _card(newer, "", f"{emails[newer]} · joined today", "bad")]  # fmt: skip
    files_stage = {"count": f"{len(members)} members in tonight's file", "cards": cards}
    aside = night["aside_members"][0]
    before, after = len(night["clean_members"]), len(night["members"])
    case = Case(
        "13", night, label="Case 13", store="members", hint="the same member twice",
        name="The same member twice, with different letter case in the email",
        where=f"{_day(day)} · the loyalty app · members {first} and {newer}", table="members",
        output="members", says_label="Before the gates, marketing had", key_label="With the gates",
        pipeline_says=f"{before} members", answer_key_says=f"{after} members",
        caught_by=["gate_clean · one person once · set aside"],
        outcome=f"{newer} is set aside by number. Marketing has one row for the person",
    )  # fmt: skip
    case.add(
        "plant", "files", "What the loyalty app sent",
        f"A customer signs up a second time, months after the first. She types her email with "
        f"capital letters. The app gives her a new member number, {newer}. No name and no full "
        "email is shown on this page: personal data must not travel.",
        "Gate", "evening", _tasks(0, "stores_send_files"), {"files": files_stage}, plant=True,
    )  # fmt: skip
    case.add(
        "before", "people", "First, the old run: two emails to one person",
        f"Both rows reach the clean table: {before} members. Marketing sends her two emails, and "
        "her points are split over two accounts. The member number is unique, and the email as "
        "typed is unique. Five green checks.",
        "Generic test", "morning", _tasks(0),
        {"files": files_stage, "people": {"count": "Marketing", "cards": [
            _card("One person", "2 emails", f"{first} and {newer}", "bad")]}}, tone="problem",
    )  # fmt: skip
    _landing_step(case, {"files": files_stage, "raw": files_stage}, "The file has every column.")
    clean_stage = {"count": f"{before} members", "cards": cards}
    case.ran("clean")
    case.add(
        "caught", "tests", 'gate_clean: the test "one person once" fails',
        "The test cleans the email first: it removes spaces and makes it lower case. Then it "
        "numbers the members that share one, oldest first. It returns the newer one. Look at the "
        "rows dbt stored: a member number, a date, and the first member's number. No email.",
        "Stored failures", "night", _tasks(4, "gate_clean"), {"clean": clean_stage}, tone="caught",
        now="one_person_once", moves=[{"from": "raw", "to": "clean"}],
        **case.caught("clean", "one_person_once", stored),
    )  # fmt: skip
    case.aside = {"count": "1 member set aside", "cards": [
        _card(aside["member_id"], "", f"{aside['reason']}. Columns: {', '.join(aside)}", "bad")]}  # fmt: skip
    left = {
        "count": f"{after} members are left",
        "cards": [_card(first, "", emails[first], "good")],
    }
    case.add(
        "aside", "quarantine", "The task acts: the newer member is set aside, by number only",
        f"{newer} leaves the clean table. The quarantine table for members has four columns, "
        "and none of them is a name, an email or a phone number. A quarantine table is read by "
        "many people. Personal data stays where it was.",
        "Quarantine", "night", _tasks(5), {"clean": left}, tone="caught",
        moves=[{"from": "clean", "to": "quarantine"}],
    )  # fmt: skip
    case.ran("publish")
    outputs = {"count": "the clean members table", "staging": "published",
               "cards": [_card("members", str(after), "one row for each person", "good")]}  # fmt: skip
    people = {
        "count": "Marketing",
        "cards": [_card("One person", "1 email", f"to member {first}", "good")],
    }
    case.add(
        "people", "people", "Marketing sends one email",
        f"Marketing reads the clean members table: {after} members, and one row for her. A person "
        "still has to merge the two accounts, so she gets all her points. The pipeline does not "
        "guess which account is the real one.",
        "Rejected rows", "morning", _tasks(8), {"outputs": outputs, "people": people},
        moves=[{"from": "clean", "to": "outputs"}],
    )  # fmt: skip
    return case.finish(people, outputs)


# ---------------------------------------------------------------- all the cases of the week


def all_cases(nights: dict, landing: Path, world, key: dict) -> list[dict]:
    sales = dict(landing=landing, key=key)
    return [
        _sales_case(
            "1",
            nights[MON],
            store="S03",
            day=MON,
            word="Singular test",
            name="A refund stored as a positive amount",
            hint="a refund stored as a positive amount",
            story="A customer brings back a bottle of olive oil and gets $9.49 back. The till "
            "knows it. The program that writes the nightly file loses the minus sign.",
            tests=["refund_gives_money_back", "line_adds_up"],
            **sales,
        ),
        _sales_case(
            "2",
            nights[TUE],
            store="S01",
            day=TUE,
            word="Stored failures",
            copies=True,
            name="One receipt in the file twice, with different line ids",
            hint="one receipt in the file twice",
            story="The export program writes one receipt of four lines twice. Each copy gets a "
            "new line id, so the old unique check sees nothing.",
            tests=["true_key_unique"],
            **sales,
        ),
        _sales_case(
            "3",
            nights[THU],
            store="S05",
            day=THU,
            word="Singular test",
            name="A weight entered in grams, not kilograms",
            hint="a weight in grams, not kilograms",
            story="A customer buys 1.180 kg of bananas for $1.52. The export program writes the "
            "scale's grams as kilograms: 1,180 kg, and $1,522.20.",
            tests=["believable_quantity"],
            **sales,
        ),
        case_4(nights[FRI], landing, key),
        _sales_case(
            "5",
            nights[SUN],
            store="S02",
            day=SUN,
            word="Severity",
            name="A sale at 03:10 in a store that closes at 22:00",
            hint="a sale at 03:10 in a store that closes at 22:00",
            story="A power cut restarts one till at 21:35, and its clock is wrong. A receipt of "
            "four lines, sold at 21:40 on Sunday, is stamped Monday 03:10.",
            tests=["inside_opening_hours"],
            **sales,
        ),
        _sales_case(
            "6",
            nights[SAT],
            store="S06",
            day=SAT,
            word="Rejected rows",
            name="A sale for a product that is not in products",
            hint="a product that is not in products",
            story="The tills already sell a new pumpkin soup. The buying team has not typed it "
            "into the price list yet. Three receipts have one each, at $5.99.",
            tests=["every_sale_has_a_product"],
            **sales,
        ),
        case_7(nights[TUE], landing, key),
        _sales_case(
            "9",
            nights[FRI],
            store="S02",
            day=FRI,
            word="Stored failures",
            copies=True,
            name="One store's whole file sent twice",
            hint="one store's whole file sent twice",
            story="The back office sends Friday's file, and twelve minutes later sends it again. "
            "The second copy has new line ids. Every sale of the day is there twice.",
            tests=["true_key_unique"],
            **sales,
        ),
        _supplier_case(
            "10",
            nights[TUE],
            landing,
            world,
            supplier="SUP-B",
            day=TUE,
            name="The supplier renames a column",
            hint="the supplier renames a column",
            story="Supplier SUP-B changes its system. The cost column is now called "
            "cost_per_unit. Nobody at Lotus is told.",
            old="{n} products of SUP-B have no cost on the order list",
        ),
        _supplier_case(
            "11",
            nights[FRI],
            landing,
            world,
            supplier="SUP-C",
            day=FRI,
            name="The supplier sends cost in cents",
            hint="the supplier sends cost in cents",
            story="Supplier SUP-C sends its costs in cents where dollars were agreed. The file "
            "has every column, and every value is a valid number: 655 where 6.55 was meant.",
            old="every cost of SUP-C is {times} times too high on the order list",
        ),
        case_12(nights[FRI], landing, world),
        case_13(nights[WED], landing),
    ]


# ---------------------------------------------------------------- two faults that block, in the lab


def _published() -> list[dict]:
    return _rows("SELECT * FROM output.daily_store_sales")


def _lab_case(cid: str, temp: Path, base: Path, landing: Path, clean_weeks: dict, *, plant: str,
              **facts) -> tuple[Case, dict, list[dict]]:  # fmt: skip
    folder = temp / f"lab_{cid}"
    _copy(base, folder)
    _point_at(folder, landing)
    before = _published()
    with connect() as wh:
        wh.execute(plant)
    night = _run_night(LAB_DAY, first="clean")
    assert night["blocked_at"], f"lab case {cid} was meant to block"
    assert _published() == before, "a blocked night must not change the published tables"
    night["report"] = clean_weeks["report"]  # the last good report is the one that still stands
    case = Case(cid, night, lab=True, **facts)
    return case, night, before


def _blocked_steps(case: Case, gate: str, test: str, stored: list[dict], before: list[dict],
                   stages: dict, text: str) -> dict:  # fmt: skip
    task = f"gate_{gate}"
    case.ran(gate)
    waiting = [
        t
        for t in ["build", "gate_publish", "publish"]
        if TASKS_AFTER[task] and t in TASKS_AFTER[task]
    ]
    case.add(
        "blocked", "tests", f'{task}: the test "{test.replace("_", " ")}" fails, with severity error',
        text, "Warn or block", "night", _tasks(TASKS.index(task), failed=task),
        stages, tone="blocked", now=test, **case.caught(gate, test, stored),
    )  # fmt: skip
    total = sum(int(r["net_sales_cents"]) for r in before)
    outputs = {"count": "daily_store_sales: the last good numbers", "staging": "nothing new is published",
               "cards": [_card(f"{len(before)} rows, as last night", money(total), "not one row changed", "good")]}  # fmt: skip
    tasks = _tasks(TASKS.index(task), failed=task)
    case.add(
        "upstream", "outputs", f"Airflow acts: {task} is failed, and {_count(len(waiting), 'task')} after it never start",
        f"The command ends with exit code 1, so Airflow marks {task} as failed. Every task after "
        f"it gets the state upstream_failed: {', '.join(waiting)}. publish does not run. The "
        f"tables people read keep their last good numbers: {len(before)} rows, {money(total)}, "
        "exactly as last night.",
        "Upstream failure", "night", tasks, {"outputs": outputs}, tone="blocked",
    )  # fmt: skip
    people = {"count": "The last report still stands", "cards": [
        _card("Head of retail operations", "", "reads last night's numbers, which were right", "good")]}  # fmt: skip
    case.add(
        "people", "people", "An old right number is better than a new wrong one",
        "Block is the strongest thing a gate can do, so it is kept for a fault that would make "
        "every number wrong. The price is real: nobody gets tonight's numbers until a person "
        "has looked.",
        "Write, audit, publish", "morning", tasks, {"outputs": outputs, "people": people},
    )  # fmt: skip
    return case.finish(people, outputs)


TASKS_AFTER = {"gate_clean": ["build", "gate_publish", "publish"], "gate_publish": ["publish"]}


def lab_cases(temp: Path, base: Path, landing: Path, clean_weeks: dict) -> list[dict]:
    # L1: a store vanishes from the stores table, exactly as tests/test_gates.py plants it.
    _point_at(base, landing)
    lines = _rows(
        "SELECT count(*) AS n, sum(amount_cents) AS cents FROM clean.sales_lines WHERE store_id = 'S06'"
    )[0]
    case, night, before = _lab_case(
        "L1", temp, base, landing, clean_weeks,
        plant="DELETE FROM raw.stores WHERE store_id = 'S06' "
        "AND _file_name = (SELECT max(_file_name) FROM raw.stores)",
        label="Planted in the lab", store="stores", hint="a store is deleted from the stores table",
        name="A store is deleted from the stores table", where="Four clean weeks · the stores table",
        table="stores", output="daily_store_sales", says_label="With no gate, S06 would lose",
        key_label="With the gate", pipeline_says=money(int(lines["cents"])),
        answer_key_says="nothing is published",
        caught_by=["gate_clean · six stores once · block"],
        outcome="gate_clean blocks. Three tasks never start, and the outputs keep their last good numbers",
    )  # fmt: skip
    stores = _rows("SELECT store_id, store_name FROM clean.stores")
    cards = [_card("S06: gone", "", "the row was deleted by mistake", "bad")] + [
        _card(r["store_id"], "", r["store_name"]) for r in stores[:3]]  # fmt: skip
    stage = {"count": f"{len(stores)} stores, not 6", "cards": cards}
    case.add(
        "plant", "files", "Somebody deletes a row from the stores table",
        "This fault is planted in the lab, on a private copy of four clean weeks. The stores "
        "table has six rows, and every sale is joined to it. One row is gone.",
        "Gate", "evening", _tasks(0), {"files": stage}, plant=True,
    )  # fmt: skip
    case.add(
        "before", "people", "With no gate: every sale of S06 is dropped by the join",
        f"{int(lines['n']):,} sales lines of S06 find no store, so the cleaning step drops them: "
        f"{money(int(lines['cents']))}. No row is left to fail a check. Wrong here is wrong "
        "everywhere.",
        "Generic test", "morning", _tasks(0), {"files": stage, "people": {"count": "Store S06", "cards": [
            _card("S06", "–", f"{money(int(lines['cents']))} of sales gone", "bad")]}}, tone="problem",
    )  # fmt: skip
    one = _blocked_steps(
        case, "clean", "six_stores_once", night["gates"]["clean"]["stored"]["six_stores_once"], before,
        {"clean": {"count": f"{len(stores)} stores, not 6", "cards": cards}},
        "For a table this small, the number of rows is a fact a person can know: six. The test "
        "returns a row because it found five. Its severity is error, and its action is block. "
        "No row can be set aside here: the bad thing is a row that is missing, and it makes "
        "every number of one store wrong.",
    )  # fmt: skip

    # L2: a valid row with a wrong quantity. Every row check passes, and the tills disagree.
    _point_at(base, landing)
    line = _rows(
        "SELECT line_id, product_id, quantity, unit_price_cents, amount_cents, line_type FROM raw.sales_lines "
        "WHERE line_id = (SELECT min(line_id) FROM raw.sales_lines WHERE quantity = '1.000' "
        "AND line_type = 'sale' AND _file_name = 'sales_lines_S03_2026-08-20.csv')"
    )[0]
    price = int(line["unit_price_cents"])
    case, night, before = _lab_case(
        "L2", temp, base, landing, clean_weeks,
        plant="UPDATE raw.sales_lines SET quantity = '2.000', "
        "amount_cents = CAST(2 * CAST(unit_price_cents AS INTEGER) AS TEXT) "
        f"WHERE line_id = '{line['line_id']}'",
        label="Planted in the lab", store="S03", hint="a valid row with a wrong quantity",
        name="A valid row with a wrong quantity", where="Thu 08-20 · store S03 · one sales line",
        table="sales_lines", output="daily_store_sales", says_label="With no gate, S03 is too high by",
        key_label="With the gate", pipeline_says=money(price), answer_key_says="nothing is published",
        caught_by=["gate_publish · unexplained difference · block"],
        outcome="Every row check passes. gate_publish blocks on a difference nobody can explain",
    )  # fmt: skip
    wrong = {**line, "quantity": "2.000", "amount_cents": 2 * price}
    stage = {"count": "One line of S03, changed", "cards": [
        _line_card(wrong, "bad", "sale · qty 2, where 1 was sold")]}  # fmt: skip
    case.add(
        "plant", "files", "A quantity of 2, where 1 was sold",
        "This fault is planted in the lab. One sales line says two pieces, and its amount is "
        "worked out again, so the line adds up. It is a real product, at today's price, inside "
        "opening hours, a believable quantity. No rule about a row is broken.",
        "Gate", "evening", _tasks(0), {"files": stage}, plant=True,
    )  # fmt: skip
    case.add(
        "before", "people", "With no gate: the store's day is too high, and nothing can say so",
        f"The day of S03 is {money(price)} too high. No check on a row can find it: the row is valid.",
        "Generic test", "morning", _tasks(0), {"files": stage, "people": {"count": "Store S03", "cards": [
            _card("S03 · Thu 08-20", "+" + money(price), "paid from a wrong number", "bad")]}}, tone="problem",
    )  # fmt: skip
    found = case.ran("clean")
    case.add(
        "clean", "tests", f"gate_clean: {len(found['tests'])} tests, and every one passes",
        "The row walks through the gate. A test can only catch what somebody thought to ask. "
        "Nobody can write a rule for every wrong value. So one more gate asks a different kind "
        "of question, about the whole day.",
        "Singular test", "night", _tasks(5), {"clean": stage}, moves=[{"from": "raw", "to": "clean"}],
    )  # fmt: skip
    stored = night["gates"]["publish"]["stored"]["unexplained_difference"]
    two = _blocked_steps(
        case, "publish", "unexplained_difference", stored, before,
        {"clean": stage, "outputs": {"count": "staging.daily_store_sales", "staging": "staging: written, not yet published",
                                     "cards": [_card("S03 · Thu 08-20", "", "the lines and the tills differ", "bad")]}},
        f"In staging, the lines of S03 no longer add up to the tills' own total. They are "
        f"{money(abs(int(stored[0]['difference_cents'])))} apart. No file is missing and no row "
        "was set aside, so nothing explains it. A difference nobody can explain means a fault no "
        "check knows. That must not be published.",
    )  # fmt: skip
    return [one, two]


# ---------------------------------------------------------------- did every planted case meet its gate?


def handled(nights: dict) -> list[int]:
    """The planted cases, 1 to 13, that the gates demonstrably acted on. From the runs."""

    def aside(day: date, store: str, file_day: date) -> bool:
        rows = nights[day]["aside_lines"] + nights[day]["aside_raw"]
        return any(r["store_id"] == store and r["file_day"] == file_day.isoformat() for r in rows)

    def failed(day: date, gate: str, test: str) -> bool:
        return any(t["name"] == test and t["failed"] for t in nights[day]["gates"][gate]["tests"])

    def status(day: date, of: date, store: str) -> str | None:
        return (nights[day]["status"].get((of.isoformat(), store)) or {}).get("problem")

    def refused(day: date, supplier: str) -> bool:
        return any(supplier in r["file_name"] for r in nights[day]["refused"])

    proof = {
        1: aside(MON, "S03", MON),
        2: aside(TUE, "S01", TUE),
        3: aside(THU, "S05", THU),
        4: failed(FRI, "clean", "price_of_the_day"),
        5: aside(SUN, "S02", SUN),
        6: aside(SAT, "S06", SAT),
        7: status(TUE, TUE, "S04") == "no_sales_file",
        8: status(THU, THU, "S06") == "no_sales_file" and status(FRI, THU, "S06") is None,
        9: aside(FRI, "S02", FRI),
        10: refused(TUE, "SUP-B"),
        11: refused(FRI, "SUP-C"),
        12: failed(FRI, "clean", "a_count_for_every_day")
        and nights[FRI]["orders"][("S05", "P-001")]["order_qty"] is None,
        13: bool(nights[WED]["aside_members"]),
    }
    return [number for number, ok in proof.items() if ok]


def false_alarms(last: dict) -> int:
    """How often a gate touched one of the three things that look wrong and are not."""
    holiday = HOLIDAY.isoformat()
    alarms = sum(
        s["status"] != "ok" for (day, _store), s in last["status"].items() if day == holiday
    )
    alarms += sum(r["file_day"] == holiday for r in last["aside_lines"] + last["aside_raw"])
    alarms += ("2026-08-27", "S01") in last["status"]  # the planned refit: no day is expected
    alarms += last["status"][("2026-08-29", "S02")]["status"] != "ok"  # the 400 bottles
    return int(alarms)


__all__ = ["all_cases", "lab_cases", "handled", "false_alarms", "EMPTY", "SAT"]
