from datetime import date

from queries.fireducks import utils

Q_NUM = 15


def q():
    utils.get_supplier_ds()
    utils.get_line_item_ds()

    def query():
        supplier = utils.get_supplier_ds()
        lineitem = utils.get_line_item_ds()

        var1 = date(1996, 1, 1)
        var2 = date(1996, 4, 1)

        revenue = (
            lineitem[(lineitem["l_shipdate"] >= var1) & (lineitem["l_shipdate"] < var2)]
            .assign(
                total_revenue=lambda df: df["l_extendedprice"] * (1 - df["l_discount"])
            )
            .groupby("l_suppkey", as_index=False)
            .agg({"total_revenue": "sum"})
        )

        q_final = (
            supplier.merge(
                revenue,
                left_on="s_suppkey",
                right_on="l_suppkey",
            )
            .pipe(lambda df: df[df["total_revenue"] == df["total_revenue"].max()])
            .assign(total_revenue=lambda df: df["total_revenue"].round(2))[
                ["s_suppkey", "s_name", "s_address", "s_phone", "total_revenue"]
            ]
            .sort_values("s_suppkey", ignore_index=True)
        )

        return q_final

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
