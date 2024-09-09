from datetime import datetime

from queries.modin import utils

Q_NUM = 12


def q():
    utils.get_line_item_ds()
    utils.get_orders_ds()

    def query():
        lineitem = utils.get_line_item_ds()
        orders = utils.get_orders_ds()

        var1 = "MAIL"
        var2 = "SHIP"
        var3 = datetime(1994, 1, 1)
        var4 = datetime(1995, 1, 1)
        high_priorities = ["1-URGENT", "2-HIGH"]

        # lineitem = lineitem.drop(columns=["comments"])
        orders = orders.drop(columns=["o_comment"])

        q_final = (
            orders.merge(lineitem, left_on="o_orderkey", right_on="l_orderkey")
            .pipe(lambda df: df[df["l_shipmode"].isin([var1, var2])])
            .pipe(lambda df: df[df["l_commitdate"] < df["l_receiptdate"]])
            .pipe(lambda df: df[df["l_shipdate"] < df["l_commitdate"]])
            .pipe(
                lambda df: df[
                    (df["l_receiptdate"] >= var3) & (df["l_receiptdate"] < var4)
                ]
            )
            .assign(
                high_line_count=lambda df: df["o_orderpriority"].isin(high_priorities),
                low_line_count=lambda df: ~df["o_orderpriority"].isin(high_priorities),
            )
            .groupby("l_shipmode", as_index=False, sort=True)
            .agg({"high_line_count": "sum", "low_line_count": "sum"})
            .astype({"high_line_count": int, "low_line_count": int})
        )
        return q_final

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
