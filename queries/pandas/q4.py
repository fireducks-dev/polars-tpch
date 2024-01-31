from datetime import datetime
from queries.pandas import utils

Q_NUM = 4


def q():
    date1 = datetime(1993, 10, 1)
    date2 = datetime(1993, 7, 1)

    # first call one time to cache in case we don't include the IO times
    utils.get_line_item_ds()
    utils.get_orders_ds()

    def query():
        lineitem = utils.get_line_item_ds()
        orders = utils.get_orders_ds()

        lineitem = lineitem[lineitem["l_commitdate"] < lineitem["l_receiptdate"]]
        orders = orders[
            (orders["o_orderdate"] < date1) & (orders["o_orderdate"] >= date2)
        ]

        result_df = (
            orders.merge(lineitem, left_on="o_orderkey", right_on="l_orderkey")
            .drop_duplicates(["o_orderpriority", "o_orderkey"])
            .groupby("o_orderpriority", as_index=False)["o_orderkey"]
            .count()
            .sort_values(["o_orderpriority"])
            .rename(columns={"o_orderkey": "order_count"})
        )
        return result_df

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
