from queries.modin import utils

Q_NUM = 9


def q():
    utils.get_part_ds()
    utils.get_supplier_ds()
    utils.get_line_item_ds()
    utils.get_part_supp_ds()
    utils.get_orders_ds()
    utils.get_nation_ds()

    def query():
        part = utils.get_part_ds()
        supplier = utils.get_supplier_ds()
        lineitem = utils.get_line_item_ds()
        partsupp = utils.get_part_supp_ds()
        orders = utils.get_orders_ds()
        nation = utils.get_nation_ds()

        supplier = supplier.drop(columns=["s_comment"])
        lineitem = lineitem.drop(columns=["comments"])
        partsupp = partsupp.drop(columns=["ps_comment"])
        orders = orders.drop(columns=["o_comment"])
        nation = nation.drop(columns=["n_comment"])

        q_final = (
            part.merge(partsupp, left_on="p_partkey", right_on="ps_partkey")
            .merge(supplier, left_on="ps_suppkey", right_on="s_suppkey")
            .merge(
                lineitem,
                left_on=["p_partkey", "ps_suppkey"],
                right_on=["l_partkey", "l_suppkey"],
            )
            .merge(orders, left_on="l_orderkey", right_on="o_orderkey")
            .merge(nation, left_on="s_nationkey", right_on="n_nationkey")
            .pipe(lambda df: df[df["p_name"].str.contains("green", regex=False)])
            .assign(
                o_year=lambda df: df["o_orderdate"].dt.year,
                amount=lambda df: df["l_extendedprice"] * (1 - df["l_discount"])
                - (df["ps_supplycost"] * df["l_quantity"]),
            )
            .rename(columns={"n_name": "nation"})
            .groupby(["nation", "o_year"], as_index=False, sort=False)
            .agg(sum_profit=("amount", "sum"))
            .sort_values(by=["nation", "o_year"], ascending=[True, False])
            .reset_index(drop=True)
        )
        return q_final

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
