from queries.pandas import utils

Q_NUM = 13


def q():
    utils.get_customer_ds()
    utils.get_orders_ds()

    def query():
        customer = utils.get_customer_ds()
        orders = utils.get_orders_ds()

        var1 = "special"
        var2 = "requests"

        orders = orders[
            ~orders["o_comment"].str.contains(f"{var1}.*{var2}", regex=True)
        ]

        q_final = (
            customer.merge(
                orders, how="left", left_on="c_custkey", right_on="o_custkey"
            )
            .groupby("c_custkey", as_index=False, sort=False)
            .agg(c_count=("o_orderkey", "count"))
            .groupby("c_count", as_index=False, sort=False)
            .agg(custdist=("c_custkey", "count"))
            .sort_values(by=["custdist", "c_count"], ascending=[False, False])
            .reset_index(drop=True)
        )

        return q_final

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
