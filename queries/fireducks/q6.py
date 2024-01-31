from datetime import datetime

import pandas as pd

from queries.fireducks import utils

Q_NUM = 6


def q():
    # first call one time to cache in case we don't include the IO times
    utils.get_line_item_ds()

    def query():
        lineitem = utils.get_line_item_ds()

        from_date = datetime(1994, 1, 1)
        to_date = datetime(1995, 1, 1)
        min_discount = 0.05
        max_discount = 0.07
        max_quantity = 24

        result = (
            lineitem[
                (lineitem["l_shipdate"] < to_date)
                & (lineitem["l_shipdate"] >= from_date)
                & (lineitem["l_discount"] <= max_discount)
                & (lineitem["l_discount"] >= min_discount)
                & (lineitem["l_quantity"] < max_quantity)
            ]
            .pipe(lambda df: df["l_extendedprice"] * df["l_discount"])
            .sum()
        )
        return pd.DataFrame({"revenue": [result]})

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
