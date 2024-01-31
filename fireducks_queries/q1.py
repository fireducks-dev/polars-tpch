from datetime import datetime
from fireducks_queries import utils

Q_NUM = 1


def q():
    # first call one time to cache in case we don't include the IO times
    utils.get_line_item_ds()

    def query():
        lineitem = utils.get_line_item_ds()

        result = (
            lineitem[lineitem["l_shipdate"] <= datetime(1998, 9, 2)]
            .assign(
                disc_price=lambda df: df["l_extendedprice"] * (1 - df["l_discount"])
            )
            .assign(charge=lambda df: df["disc_price"] * (1 + df["l_tax"]))
            .groupby(["l_returnflag", "l_linestatus"], as_index=False)
            .agg(
                sum_qty=("l_quantity", "sum"),
                sum_base_price=("l_extendedprice", "sum"),
                sum_disc_price=("disc_price", "sum"),
                sum_charge=("charge", "sum"),
                avg_qty=("l_quantity", "mean"),
                avg_price=("l_extendedprice", "mean"),
                avg_disc=("l_discount", "mean"),
                count_order=("l_returnflag", "count"),
            )
            .sort_values(["l_returnflag", "l_linestatus"])
        )

        return result

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
