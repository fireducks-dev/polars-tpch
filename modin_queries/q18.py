from modin_queries import utils

Q_NUM = 18


def q():
    utils.get_line_item_ds()
    utils.get_customer_ds()
    utils.get_orders_ds()

    def query():
        lineitem = utils.get_line_item_ds()
        customer = utils.get_customer_ds()
        orders = utils.get_orders_ds()

        result = (
            lineitem.groupby("l_orderkey", as_index=False)
            .agg(sum_quantity=("l_quantity", "sum"))
            .pipe(lambda df: df[df["sum_quantity"] > 300])
            .merge(orders, left_on="l_orderkey", right_on="o_orderkey")
            .merge(customer, left_on="o_custkey", right_on="c_custkey")
            .groupby(
                ["c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice"],
                as_index=False,
            )
            .agg(col6=("sum_quantity", "sum"))
            .sort_values(["o_totalprice", "o_orderdate"], ascending=[False, True])
            .head(100)
            .rename(columns={"o_orderdate": "o_orderdat"})
            .astype({"col6": "float64"})
        )

        return result

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
