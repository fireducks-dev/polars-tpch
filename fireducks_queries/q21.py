from fireducks_queries import utils

Q_NUM = 21


def q():
    utils.get_line_item_ds()
    utils.get_orders_ds()
    utils.get_nation_ds()
    utils.get_supplier_ds()

    def query():
        lineitem = utils.get_line_item_ds()
        orders = utils.get_orders_ds()
        nation = utils.get_nation_ds()
        supplier = utils.get_supplier_ds()

        flineitem = lineitem[lineitem["l_receiptdate"] > lineitem["l_commitdate"]]

        lineitem1 = (
            lineitem.groupby("l_orderkey", as_index=False)
            .agg(suppkey_count=("l_suppkey", "nunique"))
            .pipe(lambda df: df[df["suppkey_count"] > 1])
        )

        lineitem2 = (
            flineitem.groupby("l_orderkey", as_index=False)
            .agg(suppkey_count=("l_suppkey", "nunique"))
            .pipe(lambda df: df[df["suppkey_count"] == 1])
        )

        nation = nation[nation["n_name"] == "SAUDI ARABIA"]
        orders = orders[orders["o_orderstatus"] == "F"]

        result = (
            supplier.merge(nation, left_on="s_nationkey", right_on="n_nationkey")
            .merge(flineitem, left_on="s_suppkey", right_on="l_suppkey")
            .merge(orders, left_on="l_orderkey", right_on="o_orderkey")
            .merge(lineitem1, on="l_orderkey")
            .merge(lineitem2, on="l_orderkey")
            .groupby("s_name", as_index=False)
            .agg(numwait=("l_suppkey", "count"))
            .sort_values(["numwait", "s_name"], ascending=[False, True])
            .head(n=100)
        )
        return result

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
