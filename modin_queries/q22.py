from modin_queries import utils

Q_NUM = 22


def q():
    utils.get_customer_ds()
    utils.get_orders_ds()

    def query():
        customer = utils.get_customer_ds()
        orders = utils.get_orders_ds()

        customer["cntrycode"] = customer["c_phone"].str.slice(0, 2)
        customer = customer[
            customer["cntrycode"].isin(["13", "31", "23", "29", "30", "18", "17"])
        ]

        avg_bal = customer[customer["c_acctbal"] > 0.0]["c_acctbal"].mean()

        orders = orders.groupby("o_custkey", as_index=False).size()
        # orders = orders.drop_duplicates(subset=["o_custkey"])
        # orders = orders[["o_custkey"]].drop_duplicates(ignore_index=True)

        result = (
            customer.merge(
                orders, left_on="c_custkey", right_on="o_custkey", how="left"
            )
            .pipe(lambda df: df[df["o_custkey"].isnull()])
            .pipe(lambda df: df[df["c_acctbal"] > avg_bal])
            .groupby("cntrycode", as_index=False)
            .agg(numcust=("c_acctbal", "count"), totacctbal=("c_acctbal", "sum"))
            .sort_values("cntrycode")
            .astype({"cntrycode": "int64"})
        )

        return result

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
