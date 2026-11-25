"""The bonus report."""

from datetime import date

from lotus_pipeline import report


def test_the_week_starts_on_monday():
    assert report.week_start(date(2026, 8, 30)) == date(2026, 8, 24)
    assert report.week_start(date(2026, 8, 24)) == date(2026, 8, 24)


def test_the_bonus_is_one_percent_rounded_to_a_cent():
    assert report.bonus_cents(79618) == 796
    assert report.bonus_cents(150) == 2  # 1.5 cents rounds up


def test_money_is_written_the_same_way_everywhere():
    assert report.money(414184) == "$4,141.84"
    assert report.money(-1797) == "-$17.97"


def test_each_reason_a_day_cannot_be_paid_from_is_one_plain_line():
    day = {"sales_date": date(2026, 8, 25), "lines_cents": 0, "till_cents": 58742,
           "rows_set_aside": 0, "cents_set_aside": 0, "reasons": None}  # fmt: skip
    assert report.why({**day, "problem": "no_sales_file"}) == (
        "Tue 08-25: no sales file arrived. The tills counted $587.42"
    )
    assert report.why(
        {**day, "problem": "file_refused", "reasons": "An expected column is missing: amount_cents"}
    ) == ("Tue 08-25: the sales file was refused. An expected column is missing: amount_cents")
    assert report.why({**day, "problem": "unexplained", "lines_cents": 58000}) == (
        "Tue 08-25: the receipts and the tills are $7.42 apart, and nobody knows why"
    )
    assert report.why({**day, "problem": "rows_set_aside", "rows_set_aside": 1, "cents_set_aside": 949,
                       "reasons": "A refund gives money back"}) == (
        "Tue 08-25: 1 row set aside ($9.49): A refund gives money back"
    )  # fmt: skip
    assert report.why({**day, "problem": "no_till_total", "till_cents": None}) == (
        "Tue 08-25: no till total to compare with"
    )


def test_the_report_text():
    text = report.format_report(
        {
            "week_start": "2026-08-24", "through": "2026-08-24",
            "stores": [
                {"store_id": "S01", "store_name": "Lotus Ashby High Street", "manager_name": "Marta Lindqvist",
                 "net_sales_cents": 79524, "bonus_cents": 795, "pay": True, "why_not": []},
                {"store_id": "S03", "store_name": "Lotus Calder Bridge", "manager_name": "Tomas Reyes",
                 "net_sales_cents": 78669, "bonus_cents": 787, "pay": False,
                 "why_not": ["Mon 08-24: 1 row set aside ($9.49): A refund gives money back"]},
            ],
            "tests": 47, "tests_passed": 44, "warnings": 1,
        }
    )  # fmt: skip
    assert "$7.95   yes" in text and "DO NOT PAY YET" in text
    assert "         Mon 08-24: 1 row set aside ($9.49)" in text
    assert "1 of 2 stores can be paid." in text
    assert "44 of 47 tests passed, 1 warning." in text
