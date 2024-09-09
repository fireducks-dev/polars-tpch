from queries.modin import utils

Q_NUM = 19


def q():
    utils.get_line_item_ds()
    utils.get_part_ds()

    def query():
        lineitem = utils.get_line_item_ds()
        part = utils.get_part_ds()

        q_final = (
            part.merge(lineitem, left_on="p_partkey", right_on="l_partkey")
            .pipe(lambda df: df[df["l_shipmode"].isin(["AIR", "AIR REG"])])
            .pipe(lambda df: df[df["l_shipinstruct"] == "DELIVER IN PERSON"])
            .pipe(
                lambda df: df[
                    (
                        (df["p_brand"] == "Brand#12")
                        & df["p_container"].isin(
                            ["SM CASE", "SM BOX", "SM PACK", "SM PKG"]
                        )
                        & (df["l_quantity"] >= 1)
                        & (df["l_quantity"] <= 11)
                        & (df["p_size"] >= 1)
                        & (df["p_size"] <= 5)
                    )
                    | (
                        (df["p_brand"] == "Brand#23")
                        & df["p_container"].isin(
                            ["MED BAG", "MED BOX", "MED PKG", "MED PACK"]
                        )
                        & (df["l_quantity"] >= 10)
                        & (df["l_quantity"] <= 20)
                        & (df["p_size"] >= 1)
                        & (df["p_size"] <= 10)
                    )
                    | (
                        (df["p_brand"] == "Brand#34")
                        & df["p_container"].isin(
                            ["LG CASE", "LG BOX", "LG PACK", "LG PKG"]
                        )
                        & (df["l_quantity"] >= 20)
                        & (df["l_quantity"] <= 30)
                        & (df["p_size"] >= 1)
                        & (df["p_size"] <= 15)
                    )
                ]
            )
            .pipe(lambda df: df["l_extendedprice"] * (1 - df["l_discount"]))
            .agg(["sum"])
            .to_frame(name="revenue")
        )

        return q_final

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
