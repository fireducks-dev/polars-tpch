from datetime import datetime

from queries.fireducks import utils

Q_NUM = 8


def q():
    utils.get_customer_ds()
    utils.get_orders_ds()
    utils.get_line_item_ds()
    utils.get_part_ds()
    utils.get_supplier_ds()
    utils.get_nation_ds()
    utils.get_region_ds()

    def query():
        customer = utils.get_customer_ds()
        orders = utils.get_orders_ds()
        lineitem = utils.get_line_item_ds()
        part = utils.get_part_ds()
        supplier = utils.get_supplier_ds()
        nation = utils.get_nation_ds()
        region = utils.get_region_ds()

        from_date = datetime(1995, 1, 1)
        to_date = datetime(1996, 12, 31)
        q_nation = "BRAZIL"
        q_region = "AMERICA"
        q_part = "ECONOMY ANODIZED STEEL"

        # filters
        region = region[region["r_name"] == q_region]
        orders = orders[
            (orders["o_orderdate"] <= to_date) & (orders["o_orderdate"] >= from_date)
        ]
        part = part[part["p_type"] == q_part]

        n1 = nation[["n_nationkey", "n_regionkey"]]

        result = (
            lineitem.merge(part, left_on="l_partkey", right_on="p_partkey")
            .merge(supplier, left_on="l_suppkey", right_on="s_suppkey")
            .merge(orders, left_on="l_orderkey", right_on="o_orderkey")
            .merge(customer, left_on="o_custkey", right_on="c_custkey")
            .merge(n1, left_on="c_nationkey", right_on="n_nationkey")
            .merge(region, left_on="n_regionkey", right_on="r_regionkey")
            .merge(nation, left_on="s_nationkey", right_on="n_nationkey")
            .assign(
                volume=lambda df: df["l_extendedprice"] * (1 - df["l_discount"]),
                o_year=lambda df: df["o_orderdate"].dt.year,
                case_volume=lambda df: df["volume"].where(df["n_name"] == q_nation),
            )
            .groupby("o_year", as_index=False)
            .agg({"case_volume": "sum", "volume": "sum"})
            .assign(mkt_share=lambda df: df["case_volume"] / df["volume"])
            .round(2)
        )

        return result[["o_year", "mkt_share"]]

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
