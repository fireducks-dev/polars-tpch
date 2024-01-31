from datetime import datetime

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

        from_date = datetime(1995, 1, 1)
        to_date = datetime(1996, 12, 31)
        nation_1 = "FRANCE"
        nation_2 = "GERMANY"

        nation = nation[nation["n_name"].isin([nation_1, nation_2])]

        lineitem = lineitem[
            (lineitem["l_shipdate"] <= to_date) & (lineitem["l_shipdate"] >= from_date)
        ]

        customer = customer.merge(
            nation, left_on="c_nationkey", right_on="n_nationkey"
        ).rename(columns={"n_name": "cust_nation"})

        supplier = supplier.merge(
            nation, left_on="s_nationkey", right_on="n_nationkey"
        ).rename(columns={"n_name": "supp_nation"})

        result = (
            customer.merge(orders, left_on="c_custkey", right_on="o_custkey")
            .merge(lineitem, left_on="o_orderkey", right_on="l_orderkey")
            .merge(supplier, left_on="l_suppkey", right_on="s_suppkey")
            .pipe(lambda df: df[df["supp_nation"] != df["cust_nation"]])
            .assign(l_year=lambda df: df["l_shipdate"].dt.year)
            .assign(revenue=lambda df: df["l_extendedprice"] * (1 - df["l_discount"]))
            .groupby(
                ["supp_nation", "cust_nation", "l_year"], as_index=False, sort=True
            )
            .agg({"revenue": "sum"})
        )

        return result

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
