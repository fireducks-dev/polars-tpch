from datetime import datetime
from queries.fireducks import utils

Q_NUM = 3


def q():
    # first call one time to cache in case we don't include the IO times
    utils.get_customer_ds()
    utils.get_orders_ds()
    utils.get_line_item_ds()

    def query():
        customer = utils.get_customer_ds()
        orders = utils.get_orders_ds()
        lineitem = utils.get_line_item_ds()

        date = datetime(1995, 3, 15)
        orders = orders[orders["o_orderdate"] < date]
        customer = customer[customer["c_mktsegment"] == "BUILDING"]
        lineitem = lineitem[lineitem["l_shipdate"] > date]

        result = (
            customer.merge(orders, left_on="c_custkey", right_on="o_custkey")
            .merge(lineitem, left_on="o_orderkey", right_on="l_orderkey")
            .assign(revenue=lambda df: df["l_extendedprice"] * (1 - df["l_discount"]))
            .groupby(["l_orderkey", "o_orderdate", "o_shippriority"], as_index=False)
            .agg({"revenue": "sum"})[
                ["l_orderkey", "revenue", "o_orderdate", "o_shippriority"]
            ]
            .sort_values(["revenue", "o_orderdate"], ascending=[False, True])
            .head(10)
        )
        return result

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
