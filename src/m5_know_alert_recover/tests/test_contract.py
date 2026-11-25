"""The data contract for the suppliers' file, and the checker that holds a file against it."""

from lotus_pipeline import contract, load, pipeline


def test_the_contract_has_its_four_parts():
    agreed = contract.load_contract()
    assert set(agreed) >= {"owner", "columns", "change_policy", "version"}
    assert [c["name"] for c in agreed["columns"]] == load.TABLES["supplier_prices"]
    assert set(agreed["change_policy"]) == {"additive", "breaking"}


def test_thirteen_files_keep_the_contract_and_two_break_it(landing):
    results = {r["file"]: r for r in contract.check_landing(landing)}
    assert len(results) == 15
    refused = sorted(name for name, r in results.items() if r["verdict"] == "refuse")
    assert refused == [
        "supplier_prices_SUP-B_2026-08-25.csv",
        "supplier_prices_SUP-C_2026-08-28.csv",
    ]
    assert all(r["problems"] == [] for name, r in results.items() if name not in refused)


def test_case_10_a_renamed_column_is_one_breaking_and_one_additive_change(landing):
    path = landing / "supplier_prices" / "supplier_prices_SUP-B_2026-08-25.csv"
    found = [(p["kind"], p["column"]) for p in contract.check_file(path)]
    assert found == [("breaking", "unit_cost"), ("additive", "cost_per_unit")]


def test_case_11_cents_break_the_rule_of_the_column(landing):
    path = landing / "supplier_prices" / "supplier_prices_SUP-C_2026-08-28.csv"
    problems = contract.check_file(path)
    assert len(problems) == 15 and {p["column"] for p in problems} == {"unit_cost"}
    assert all("0 decimals, not 2" in p["problem"] for p in problems)


def test_each_rule_of_a_value(tmp_path):
    def problems(line: str) -> list[str]:
        path = tmp_path / "supplier_prices_SUP-A_2026-08-24.csv"
        path.write_text("supplier_id,product_id,unit_cost,unit,price_date\n" + line + "\n")
        return [p["problem"] for p in contract.check_file(path)]

    assert problems("SUP-A,P-001,1.75,each,2026-08-24") == []
    assert problems("SUP-Z,P-001,1.75,each,2026-08-24") == ["not one of SUP-A, SUP-B, SUP-C"]
    assert problems("SUP-A,P1,1.75,each,2026-08-24") == ["not shaped like P-000"]
    assert problems("SUP-A,P-001,,each,2026-08-24") == ["the cell is empty"]
    assert problems("SUP-A,P-001,cheap,each,2026-08-24") == ["not a number"]
    assert problems("SUP-A,P-001,0.00,each,2026-08-24") == [
        "below the smallest allowed value, 0.01"
    ]
    assert problems("SUP-A,P-001,75.00,each,2026-08-24") == [
        "above the largest allowed value, 50.0"
    ]
    assert problems("SUP-A,P-001,1.75,box,2026-08-24") == ["not one of each, kg"]
    assert problems("SUP-A,P-001,1.75,each,24/08/2026") == ["not a date like 2026-08-25"]
    assert problems("SUP-A,P-001,1.75,each,2026-08-23") == [
        "not the date in the file name, 2026-08-24"
    ]


def test_an_extra_column_is_accepted_and_reported(tmp_path):
    path = tmp_path / "supplier_prices_SUP-A_2026-08-24.csv"
    path.write_text(
        "supplier_id,product_id,unit_cost,unit,price_date,note\nSUP-A,P-001,1.75,each,2026-08-24,hi\n"
    )
    found = contract.check_file(path)
    assert [p["kind"] for p in found] == ["additive"]
    assert contract.verdict(found) == "accept, and report"
    assert contract.verdict([]) == "accept"


def test_the_command_line(own_warehouse, capsys, landing):
    assert pipeline.main(["contract"]) == 0
    out = capsys.readouterr().out
    assert "15 files. 2 would be refused at the door." in out
    one = str(landing / "supplier_prices" / "supplier_prices_SUP-A_2026-08-24.csv")
    assert pipeline.main(["contract", "--file", one]) == 0
    assert "accept" in capsys.readouterr().out
