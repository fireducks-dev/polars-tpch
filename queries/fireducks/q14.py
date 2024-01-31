from datetime import datetime

import pandas as pd
from queries.fireducks import utils

Q_NUM = 14


def q():
    utils.get_line_item_ds()
    utils.get_part_ds()

    def query():
        lineitem = utils.get_line_item_ds()
        part = utils.get_part_ds()

        from_date = datetime(1995, 9, 1)
        to_date = datetime(1995, 10, 1)
        startstr = "PROMO"

        result = (
            lineitem[
                (lineitem["l_shipdate"] < to_date)
                & (lineitem["l_shipdate"] >= from_date)
            ]
            .merge(part, left_on="l_partkey", right_on="p_partkey")
            .assign(revenue=lambda df: df["l_extendedprice"] * (1 - df["l_discount"]))
            .assign(
                promo_revenue=lambda df: df["revenue"].where(
                    df["p_type"].str.startswith(startstr)
                )
            )[["revenue", "promo_revenue"]]
            .sum()
            .pipe(lambda s: (100.0 * s["promo_revenue"] / s["revenue"]))
        )

        return pd.DataFrame({"promo_revenue": [result]}).round(2)

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
