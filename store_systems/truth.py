"""The answer key, worked out straight from the till journal.

It does not use any pipeline code, and it never reads a file a store sent.
Tests compare every pipeline against it.
"""

from datetime import date, timedelta
from decimal import Decimal

from store_systems.catalog import BONUS_RATE_PERCENT, PRODUCTS, STORES, is_open, on_sale
from store_systems.generate import World, order_quantity, units_sold


def daily_store_sales(world: World) -> dict[tuple[date, str], dict]:
    """The true number for every store and day: (day, store) -> receipts, lines, net sales."""
    days: dict[tuple[date, str], dict] = {}
    for receipt in world.receipts:
        row = days.setdefault(
            (receipt.business_date, receipt.store_id),
            {"receipts": 0, "lines": 0, "net_sales_cents": 0},
        )
        row["receipts"] += 1
        row["lines"] += len(receipt.lines)
        row["net_sales_cents"] += receipt.total_cents  # a refund is negative, so it takes away
    return days


def week_net_sales(world: World, monday: date, through: date | None = None) -> dict[str, int]:
    """Each store's true net sales from `monday` up to `through`, or for the whole week."""
    last = through or monday + timedelta(days=6)
    totals = {store_id: 0 for store_id in STORES}
    for (day, store_id), row in daily_store_sales(world).items():
        if monday <= day <= last:
            totals[store_id] += row["net_sales_cents"]
    return totals


def bonus_cents(net_sales_cents: int) -> int:
    """A manager's bonus: 1% of net sales, rounded to a cent."""
    return (net_sales_cents * BONUS_RATE_PERCENT + 50) // 100


def true_costs(world: World, day: date) -> dict[str, Decimal]:
    """The newest true cost of each product on `day`, in dollars."""
    newest: dict[str, tuple[date, Decimal]] = {}
    for (_supplier, price_date), costs in world.costs.items():
        if price_date > day:
            continue
        for product_id, cost in costs.items():
            if product_id not in newest or price_date > newest[product_id][0]:
                newest[product_id] = (price_date, cost)
    return {product_id: cost for product_id, (_d, cost) in newest.items()}


def replenishment_list(world: World, day: date) -> dict[tuple[str, str], dict]:
    """The true order list made after business day `day`: (store, product) -> the order."""
    sold = units_sold(world.receipts)
    costs = true_costs(world, day)
    orders = {}
    for store_id in STORES:
        for product_id in PRODUCTS:
            if not on_sale(product_id, day) or not is_open(store_id, day):
                continue
            sold_yesterday = sold.get((day, store_id, product_id), Decimal(0))
            stock_counted = world.stock[(day, store_id, product_id)]["counted_qty"]
            order_qty = order_quantity(sold_yesterday, stock_counted)
            unit_cost_cents = int(costs[product_id] * 100)
            orders[(store_id, product_id)] = {
                "sold_yesterday": sold_yesterday,
                "stock_counted": stock_counted,
                "order_qty": order_qty,
                "unit_cost_cents": unit_cost_cents,
            }
    return orders


def answer_key(world: World) -> dict:
    """The whole key, as plain values that can be saved in a file."""
    sales = daily_store_sales(world)
    return {
        "daily_store_sales": [
            {"sales_date": day.isoformat(), "store_id": store_id, **row}
            for (day, store_id), row in sorted(sales.items())
        ],
        "planted_receipts": {
            key: {
                "receipt_id": receipt.receipt_id,
                "store_id": receipt.store_id,
                "sold_at": receipt.sold_at.strftime("%Y-%m-%d %H:%M:%S"),
                "total_cents": receipt.total_cents,
            }
            for key, receipt in world.planted.items()
        },
    }
