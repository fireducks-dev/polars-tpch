from datetime import datetime
from queries.pandas import utils

Q_NUM = 4


def q():
    # first call one time to cache in case we don't include the IO times
    utils.get_line_item_ds()
    utils.get_orders_ds()

    def query():
        lineitem = utils.get_line_item_ds()
        orders = utils.get_orders_ds()
        orders = orders.drop(columns=["o_comment"])

        var1 = datetime(1993, 7, 1)
        var2 = datetime(1993, 10, 1)

        q_final = (
            orders.merge(lineitem, left_on="o_orderkey", right_on="l_orderkey")
            .pipe(
                lambda df: df[(df["o_orderdate"] < var2) & (df["o_orderdate"] >= var1)]
            )
            .pipe(lambda df: df[df["l_commitdate"] < df["l_receiptdate"]])
            .drop_duplicates(["o_orderpriority", "o_orderkey"])
            .groupby("o_orderpriority", as_index=False)["o_orderkey"]
            .count()
            .sort_values(["o_orderpriority"])
            .rename(columns={"o_orderkey": "order_count"})
        )
        return q_final

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
