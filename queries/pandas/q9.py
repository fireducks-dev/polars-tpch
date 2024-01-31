from queries.pandas import utils

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

        q_color = "green"
        part = part[part["p_name"].str.contains(q_color, regex=False)]

        result = (
            lineitem.merge(part, left_on="l_partkey", right_on="p_partkey")
            .merge(
                supplier.merge(nation, left_on="s_nationkey", right_on="n_nationkey"),
                left_on="l_suppkey",
                right_on="s_suppkey",
            )
            .merge(
                partsupp,
                left_on=["l_suppkey", "l_partkey"],
                right_on=["ps_suppkey", "ps_partkey"],
            )
            .merge(orders, left_on="l_orderkey", right_on="o_orderkey")
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
        return result

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
