from datetime import datetime

from fireducks_queries import utils

Q_NUM = 12


def q():
    utils.get_line_item_ds()
    utils.get_orders_ds()

    def query():
        lineitem = utils.get_line_item_ds()
        orders = utils.get_orders_ds()

        from_date = datetime(1994, 1, 1)
        to_date = datetime(1995, 1, 1)
        q_shipmodes = ["MAIL", "SHIP"]
        high_priorities = ["1-URGENT", "2-HIGH"]

        lineitem = lineitem[
            (lineitem["l_shipmode"].isin(q_shipmodes))
            & (lineitem["l_shipdate"] < lineitem["l_commitdate"])
            & (lineitem["l_commitdate"] < lineitem["l_receiptdate"])
            & (lineitem["l_receiptdate"] >= from_date)
            & (lineitem["l_receiptdate"] < to_date)
        ]

        merged = lineitem.merge(orders, left_on="l_orderkey", right_on="o_orderkey")

        is_high_priority = merged["o_orderpriority"].isin(high_priorities)
        merged["high_line_count"] = is_high_priority
        merged["low_line_count"] = ~is_high_priority

        result = (
            merged.groupby("l_shipmode", as_index=False, sort=True)
            .agg({"high_line_count": "sum", "low_line_count": "sum"})
            .astype({"high_line_count": int, "low_line_count": int})
        )
        return result

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
