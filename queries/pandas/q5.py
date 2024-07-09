from datetime import datetime
from queries.pandas import utils

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

        # lineitem = lineitem.drop(columns=["comments"])
        # orders = orders.drop(columns=["o_comment"])
        customer = customer.drop(columns=["c_comment"])

        var1 = "ASIA"
        var2 = datetime(1994, 1, 1)
        var3 = datetime(1995, 1, 1)

        q_final = (
            region.merge(nation, left_on="r_regionkey", right_on="n_regionkey")
            .merge(customer, left_on="n_nationkey", right_on="c_nationkey")
            .merge(orders, left_on="c_custkey", right_on="o_custkey")
            .merge(lineitem, left_on="o_orderkey", right_on="l_orderkey")
            .merge(
                supplier,
                left_on=["l_suppkey", "n_nationkey"],
                right_on=["s_suppkey", "s_nationkey"],
            )
            .pipe(lambda df: df[df["r_name"] == var1])
            .pipe(
                lambda df: df[
                    # df["o_orderdate"].between(from_date, to_date, inclusive="left")
                    (df["o_orderdate"] < var3) & (df["o_orderdate"] >= var2)
                ]
            )
            .assign(revenue=lambda df: df["l_extendedprice"] * (1 - df["l_discount"]))
            .groupby("n_name", as_index=False)
            .agg({"revenue": "sum"})
            .sort_values("revenue", ascending=[False])
        )

        return q_final

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
