"""Who uses each table, and what happens to them when it is wrong.

This is the question to ask before any check is written. Three small maps answer it:

    BUILT_FROM   which table is built from which: the map of what reads what
    CONSUMERS    the people, the tables they read, and what a wrong value costs them
    MADE_BY      how each of the seven source tables is made

From them, `impact()` says who is hurt when one table is wrong, and `rank_tables()`
puts the seven tables in order of harm.
"""

SOURCE_TABLES = [
    "sales_lines", "till_totals", "products", "stores", "stock_counts", "supplier_prices",
    "members",
]  # fmt: skip

# table -> the tables it is built from. Read off clean.py and publish.py.
BUILT_FROM = {
    "clean.sales_lines": ["raw.sales_lines", "clean.products", "clean.stores"],
    "clean.till_totals": ["raw.till_totals"],
    "clean.products": ["raw.products"],
    "clean.stores": ["raw.stores"],
    "clean.stock_counts": ["raw.stock_counts"],
    "clean.supplier_prices": ["raw.supplier_prices"],
    "clean.members": ["raw.members"],
    "output.daily_store_sales": ["clean.sales_lines"],
    "output.replenishment_list": [
        "clean.stores", "clean.products", "clean.sales_lines", "clean.stock_counts",
        "clean.supplier_prices",
    ],
}  # fmt: skip

CONSUMERS = [
    {
        "who": "Store managers",
        "reads": ["output.daily_store_sales"],
        "decides": "Nothing. They are paid from it: a weekly bonus of 1% of net sales",
        "harm": "A wrong number is wrong pay",
    },
    {
        "who": "Head of retail operations",
        "reads": ["output.daily_store_sales"],
        "decides": "Whether to sign off the bonus on Monday",
        "harm": "She signs a wrong number, and cannot say which one",
    },
    {
        "who": "The replenishment team",
        "reads": ["output.replenishment_list"],
        "decides": "How much of each product to order for tomorrow",
        "harm": "Too high wastes food. Too low leaves empty shelves",
    },
    {
        "who": "Finance",
        "reads": ["output.replenishment_list", "clean.supplier_prices"],
        "decides": "How much to pay each supplier",
        "harm": "A wrong price is wrong money leaving the company",
    },
    {
        "who": "Marketing",
        "reads": ["clean.members"],
        "decides": "Who gets an email, and at which address",
        "harm": "One person stored twice gets two emails. A wrong address reaches a stranger",
    },
]

MADE_BY = {
    "sales_lines": "Each till writes it. Each store sends a file every night",
    "till_totals": "The till prints it when it is closed",
    "products": "Typed by the buying team",
    "stores": "Typed once, rarely changed",
    "stock_counts": "Counted by hand or by scanner",
    "supplier_prices": "A CSV file the supplier sends",
    "members": "Typed by the customer in an app",
}


def downstream(table: str) -> list[str]:
    """Every table built from `table`, directly or through another table."""
    found: list[str] = []
    todo = [table]
    while todo:
        current = todo.pop()
        for built, parts in BUILT_FROM.items():
            if current in parts and built not in found:
                found.append(built)
                todo.append(built)
    return sorted(found)


def impact(source_table: str) -> dict:
    """Everything a wrong source table touches: the tables, and the people who read them."""
    tables = downstream(f"raw.{source_table}")
    people = [c["who"] for c in CONSUMERS if set(c["reads"]) & set(tables)]
    return {
        "table": source_table,
        "made_by": MADE_BY[source_table],
        "tables": tables,
        "outputs": [t for t in tables if t.startswith("output.")],
        "people": people,
    }


def rank_tables() -> list[dict]:
    """The seven tables, the most harmful first: by how many groups of people a fault reaches."""
    ranked = sorted(
        (impact(table) for table in SOURCE_TABLES),
        key=lambda i: (-len(i["people"]), -len(i["outputs"]), SOURCE_TABLES.index(i["table"])),
    )
    for position, row in enumerate(ranked, start=1):
        row["rank"] = position
        row["critical"] = len(row["people"]) >= 3
    return ranked


def format_impact(found: dict) -> str:
    lines = [
        f"{found['table']}: {found['made_by']}.",
        "  A wrong value here reaches these tables:",
        *[f"    {t}" for t in found["tables"]],
        "  and these people:",
        *([f"    {p}" for p in found["people"]] or ["    nobody. No person reads it yet"]),
    ]
    return "\n".join(lines)
