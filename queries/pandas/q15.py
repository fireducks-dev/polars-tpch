from datetime import datetime

from queries.pandas import utils

Q_NUM = 15


def q():
    utils.get_supplier_ds()
    utils.get_line_item_ds()

    def query():
        supplier = utils.get_supplier_ds()
        lineitem = utils.get_line_item_ds()

        revenue = (
            lineitem[
                (lineitem["l_shipdate"] >= datetime(1996, 1, 1))
                & (lineitem["l_shipdate"] < datetime(1996, 4, 1))
            ]
            .assign(
                total_revenue=lambda df: df["l_extendedprice"] * (1 - df["l_discount"])
            )
            .groupby("l_suppkey", as_index=False)
            .agg({"total_revenue": "sum"})
        )

        revenue = revenue[
            revenue["total_revenue"] == revenue["total_revenue"].max()
        ].round(2)

        result = supplier.merge(
            revenue,
            left_on="s_suppkey",
            right_on="l_suppkey",
        )[["s_suppkey", "s_name", "s_address", "s_phone", "total_revenue"]].sort_values(
            "s_suppkey", ignore_index=True
        )

        return result

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
