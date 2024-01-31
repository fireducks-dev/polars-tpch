from datetime import datetime
from fireducks_queries import utils

Q_NUM = 5


def q():
    utils.get_customer_ds()
    utils.get_orders_ds()
    utils.get_line_item_ds()
    utils.get_supplier_ds()
    utils.get_nation_ds()
    utils.get_region_ds()

    def query():
        customer = utils.get_customer_ds()
        orders = utils.get_orders_ds()
        lineitem = utils.get_line_item_ds()
        supplier = utils.get_supplier_ds()
        nation = utils.get_nation_ds()
        region = utils.get_region_ds()

        from_date = datetime(1994, 1, 1)
        to_date = datetime(1995, 1, 1)
        orders = orders[
            (orders["o_orderdate"] < to_date) & (orders["o_orderdate"] >= from_date)
        ]
        region = region[region["r_name"] == "ASIA"]

        result = (
            region.merge(nation, left_on="r_regionkey", right_on="n_regionkey")
            .merge(customer, left_on="n_nationkey", right_on="c_nationkey")
            .merge(orders, left_on="c_custkey", right_on="o_custkey")
            .merge(lineitem, left_on="o_orderkey", right_on="l_orderkey")
            .merge(
                supplier,
                left_on=["l_suppkey", "n_nationkey"],
                right_on=["s_suppkey", "s_nationkey"],
            )
            .assign(revenue=lambda df: df["l_extendedprice"] * (1 - df["l_discount"]))
            .groupby("n_name", as_index=False)
            .agg({"revenue": "sum"})
            .sort_values("revenue", ascending=[False])
        )

        return result

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
