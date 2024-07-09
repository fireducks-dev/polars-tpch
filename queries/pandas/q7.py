from datetime import datetime

import pandas as pd

from queries.pandas import utils

Q_NUM = 7


def q():
    utils.get_customer_ds()
    utils.get_line_item_ds()
    utils.get_nation_ds()
    utils.get_orders_ds()
    utils.get_supplier_ds()


    def query():
        customer = utils.get_customer_ds()
        lineitem = utils.get_line_item_ds()
        nation = utils.get_nation_ds()
        orders = utils.get_orders_ds()
        supplier = utils.get_supplier_ds()

        var1 = "FRANCE"
        var2 = "GERMANY"
        var3 = datetime(1995, 1, 1)
        var4 = datetime(1996, 12, 31)

        n1 = nation[nation["n_name"] == var1]
        n2 = nation[nation["n_name"] == var2]

        # lineitem = lineitem.pipe(
        #     lambda df: df[(df["l_shipdate"] >= var3) & (df["l_shipdate"] <= var4)]
        # )

        q1 = (
            customer.merge(n1, left_on="c_nationkey", right_on="n_nationkey")
            .merge(orders, left_on="c_custkey", right_on="o_custkey")
            .rename(columns={"n_name": "cust_nation"})
            .merge(lineitem, left_on="o_orderkey", right_on="l_orderkey")
            .merge(supplier, left_on="l_suppkey", right_on="s_suppkey")
            .merge(n2, left_on="s_nationkey", right_on="n_nationkey")
            .rename(columns={"n_name": "supp_nation"})
        )

        q2 = (
            customer.merge(n2, left_on="c_nationkey", right_on="n_nationkey")
            .merge(orders, left_on="c_custkey", right_on="o_custkey")
            .rename(columns={"n_name": "cust_nation"})
            .merge(lineitem, left_on="o_orderkey", right_on="l_orderkey")
            .merge(supplier, left_on="l_suppkey", right_on="s_suppkey")
            .merge(n1, left_on="s_nationkey", right_on="n_nationkey")
            .rename(columns={"n_name": "supp_nation"})
        )

        q_final = (
            pd.concat([q1, q2])
            .pipe(
                lambda df: df[(df["l_shipdate"] >= var3) & (df["l_shipdate"] <= var4)]
            )
            .assign(l_year=lambda df: df["l_shipdate"].dt.year)
            .assign(revenue=lambda df: df["l_extendedprice"] * (1 - df["l_discount"]))
            .groupby(
                ["supp_nation", "cust_nation", "l_year"], as_index=False, sort=True
            )
            .agg({"revenue": "sum"})
        )

        return q_final

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
