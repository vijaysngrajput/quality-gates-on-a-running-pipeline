"""The bonus report."""

from datetime import date

from lotus_pipeline import report


def test_the_week_starts_on_monday():
    assert report.week_start(date(2026, 8, 30)) == date(2026, 8, 24)
    assert report.week_start(date(2026, 8, 24)) == date(2026, 8, 24)


def test_the_bonus_is_one_percent_rounded_to_a_cent():
    assert report.bonus_cents(79618) == 796
    assert report.bonus_cents(77720) == 777
    assert report.bonus_cents(150) == 2  # 1.5 cents rounds up


def test_money_is_written_the_same_way_everywhere():
    assert report.money(414184) == "$4,141.84"
    assert report.money(-1797) == "-$17.97"
    assert report.money(5) == "$0.05"


def test_the_report_text():
    text = report.format_report(
        {
            "week_start": "2026-08-24",
            "through": "2026-08-24",
            "stores": [
                {"store_id": "S03", "store_name": "Lotus Calder Bridge",
                 "manager_name": "Tomas Reyes", "net_sales_cents": 79618, "bonus_cents": 796,
                 "days_reported": 1},
            ],
            "checks_total": 5,
            "checks_passed": 5,
        }
    )  # fmt: skip
    assert "week of Mon 2026-08-24" in text
    assert "$796.18" in text and "$7.96" in text
    assert "Checks: 5 of 5 passed." in text
