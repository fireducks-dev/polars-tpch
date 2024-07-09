from datetime import datetime

import pandas as pd

from queries.pandas import utils

Q_NUM = 6


def q():
    # first call one time to cache in case we don't include the IO times
    utils.get_line_item_ds()

    def query():
        lineitem = utils.get_line_item_ds()

        var1 = datetime(1994, 1, 1)
        var2 = datetime(1995, 1, 1)
        var3 = 0.05
        var4 = 0.07
        var5 = 24

        result = (
            lineitem[
                (lineitem["l_shipdate"] < var2)
                & (lineitem["l_shipdate"] >= var1)
                & (lineitem["l_discount"] <= var4)
                & (lineitem["l_discount"] >= var3)
                & (lineitem["l_quantity"] < var5)
            ]
            .pipe(lambda df: df["l_extendedprice"] * df["l_discount"])
            .sum()
        )
        return pd.DataFrame({"revenue": [result]})

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
