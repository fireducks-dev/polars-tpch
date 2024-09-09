from datetime import datetime

from queries.modin import utils

Q_NUM = 10


def q():
    utils.get_customer_ds()
    utils.get_orders_ds()
    utils.get_line_item_ds()
    utils.get_nation_ds()

    def query():
        customer = utils.get_customer_ds()
        orders = utils.get_orders_ds()
        lineitem = utils.get_line_item_ds()
        nation = utils.get_nation_ds()

        var1 = datetime(1993, 10, 1)
        var2 = datetime(1994, 1, 1)

        result = (
            customer.merge(orders, left_on="c_custkey", right_on="o_custkey")
            .merge(lineitem, left_on="o_orderkey", right_on="l_orderkey")
            .merge(nation, left_on="c_nationkey", right_on="n_nationkey")
            .pipe(
                lambda df: df[(df["o_orderdate"] < var2) & (df["o_orderdate"] >= var1)]
            )
            .pipe(lambda df: df[df["l_returnflag"] == "R"])
            .assign(volume=lambda df: df["l_extendedprice"] * (1 - df["l_discount"]))
            .groupby(
                [
                    "c_custkey",
                    "c_name",
                    "c_acctbal",
                    "c_phone",
                    "n_name",
                    "c_address",
                    "c_comment",
                ],
                as_index=False,
                sort=False,
            )
            .agg(revenue=("volume", "sum"))[
                [
                    "c_custkey",
                    "c_name",
                    "revenue",
                    "c_acctbal",
                    "n_name",
                    "c_address",
                    "c_phone",
                    "c_comment",
                ]
            ]
            .sort_values(by="revenue", ascending=False)
            .reset_index(drop=True)
            .head(20)
        )

        return result

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
