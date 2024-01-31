from modin_queries import utils
from datetime import datetime

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

        from_date = datetime(1993, 10, 1)
        to_date = datetime(1994, 1, 1)
        q_flag = "R"
        limit = 20

        lineitem = lineitem[lineitem["l_returnflag"] == q_flag]
        orders = orders[
            (orders["o_orderdate"] < to_date) & (orders["o_orderdate"] >= from_date)
        ]

        result = (
            orders.merge(customer, left_on="o_custkey", right_on="c_custkey")
            .merge(nation, left_on="c_nationkey", right_on="n_nationkey")
            .merge(lineitem, left_on="o_orderkey", right_on="l_orderkey")
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
            .agg(revenue=("volume", "sum"))
            .sort_values(by="revenue", ascending=False)
            .reset_index(drop=True)
            .head(limit)
        )

        return result

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
