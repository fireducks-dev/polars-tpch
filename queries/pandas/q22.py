from queries.pandas import utils

Q_NUM = 22


def q():
    utils.get_customer_ds()
    utils.get_orders_ds()

    def query():
        customer = utils.get_customer_ds()
        orders = utils.get_orders_ds()

        q1 = customer.assign(cntrycode=customer["c_phone"].str.slice(0, 2)).pipe(
            lambda df: df[
                df["cntrycode"].isin(["13", "31", "23", "29", "30", "18", "17"])
            ]
        )

        q2 = q1[q1["c_acctbal"] > 0.0]["c_acctbal"].mean()

        # q3 = orders.groupby("o_custkey", as_index=False).size()
        q3 = orders.drop_duplicates(subset=["o_custkey"])
        # q3 = orders[["o_custkey"]].drop_duplicates(ignore_index=True)

        q_final = (
            q1.merge(q3, left_on="c_custkey", right_on="o_custkey", how="left")
            .pipe(lambda df: df[df["o_custkey"].isnull()])
            .pipe(lambda df: df[df["c_acctbal"] > q2])
            .groupby("cntrycode", as_index=False)
            .agg(numcust=("c_acctbal", "count"), totacctbal=("c_acctbal", "sum"))
            .sort_values("cntrycode")
        )

        return q_final

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
