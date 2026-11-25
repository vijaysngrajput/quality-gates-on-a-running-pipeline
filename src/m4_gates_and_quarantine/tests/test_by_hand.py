"""The number by hand, held up against the pipeline and the answer key."""

from datetime import date

from lotus_pipeline import by_hand, pipeline
from lotus_pipeline.warehouse import connect

DAY = date(2026, 8, 24)


def test_by_hand_equals_the_answer_key_for_every_store(landing, key):
    for store in ("S01", "S02", "S03", "S04", "S05", "S06"):
        lines = by_hand.read_lines(store, DAY, landing)
        got = by_hand.net_sales_by_hand(lines)
        assert got["net_sales_cents"] == key[(DAY, store)]["net_sales_cents"]
        assert got["receipts"] == key[(DAY, store)]["receipts"]
        assert by_hand.till_total(store, DAY, landing) == got["net_sales_cents"]


def test_a_refund_is_taken_away_whatever_its_sign():
    lines = [
        {"receipt_id": "R1", "line_type": "sale", "amount_cents": "1000"},
        {"receipt_id": "R2", "line_type": "refund", "amount_cents": "-300"},
        {"receipt_id": "R3", "line_type": "refund", "amount_cents": "200"},  # the planted kind
    ]
    assert by_hand.net_sales_by_hand(lines) == {
        "receipts": 3, "lines": 3, "sales_cents": 1000, "refunds_cents": 500,
        "net_sales_cents": 500,
    }  # fmt: skip


def test_the_gate_set_the_bad_refund_aside_so_the_day_is_still_not_the_hand_count(
    own_warehouse, landing, world
):
    """By hand: $777.20. Published: $786.69. The refund is set aside, so it is not taken away yet."""
    pipeline.run(DAY, say=lambda text: None)
    with connect() as wh:
        found = by_hand.compare(wh, "S03", DAY, landing)
        clean = by_hand.compare(wh, "S01", DAY, landing)
    assert (found["pipeline_cents"], found["by_hand_cents"]) == (78669, 77720)
    assert found["till_total_cents"] == 77720
    assert found["difference_cents"] == 949
    assert [s["receipt_id"] for s in found["suspects"]] == [world.planted["c1_refund"].receipt_id]
    assert clean["difference_cents"] == 0 and clean["suspects"] == []


def test_the_command_line(own_warehouse, capsys):
    pipeline.run(DAY, say=lambda text: None)
    assert pipeline.main(["by-hand", "--store", "S03", "--day", "2026-08-24"]) == 0
    out = capsys.readouterr().out
    assert "$786.69" in out and "$777.20" in out and "$9.49" in out
