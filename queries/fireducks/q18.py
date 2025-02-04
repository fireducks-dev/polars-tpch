from queries.fireducks import utils

Q_NUM = 18


def q():
    utils.get_line_item_ds()
    utils.get_customer_ds()
    utils.get_orders_ds()

    def query():
        lineitem = utils.get_line_item_ds()
        customer = utils.get_customer_ds()
        orders = utils.get_orders_ds()

        var1 = 300

        q1 = (
            lineitem.groupby("l_orderkey", as_index=False)
            .agg(sum_quantity=("l_quantity", "sum"))
            .pipe(lambda df: df[df["sum_quantity"] > var1])
        )

        q_final = (
            orders.merge(q1, left_on="o_orderkey", right_on="l_orderkey")
            .merge(lineitem, left_on="o_orderkey", right_on="l_orderkey")
            .merge(customer, left_on="o_custkey", right_on="c_custkey")
            .groupby(
                ["c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice"],
                as_index=False,
            )
            .agg(col6=("l_quantity", "sum"))
            .sort_values(["o_totalprice", "o_orderdate"], ascending=[False, True])
            .rename(columns={"o_orderdate": "o_orderdat"})
            .astype({"col6": "float64"})
            .head(100)
        )

        return q_final

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
