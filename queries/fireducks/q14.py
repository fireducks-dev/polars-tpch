from datetime import date

import pandas as pd

from queries.fireducks import utils

Q_NUM = 14


def q():
    utils.get_line_item_ds()
    utils.get_part_ds()

    def query():
        lineitem = utils.get_line_item_ds()
        part = utils.get_part_ds()

        var1 = date(1995, 9, 1)
        var2 = date(1995, 10, 1)

        q_final = (
            lineitem.merge(part, left_on="l_partkey", right_on="p_partkey")
            .pipe(lambda df: df[(df["l_shipdate"] < var2) & (df["l_shipdate"] >= var1)])
            .pipe(
                lambda df: (
                    100.00
                    * (df["l_extendedprice"] * (1 - df["l_discount"]))
                    .where(df["p_type"].str.startswith("PROMO"))
                    .agg(["sum"])
                    / (df["l_extendedprice"] * (1 - df["l_discount"])).agg(["sum"])
                )
            )
            .round(2)
            .to_frame(name="promo_revenue")
        )

        return q_final

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
