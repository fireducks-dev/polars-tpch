from queries.fireducks import utils

Q_NUM = 17


def q():
    utils.get_line_item_ds()
    utils.get_part_ds()

    def query():
        lineitem = utils.get_line_item_ds()
        part = utils.get_part_ds()

        part = part[
            (part["p_brand"] == "Brand#23") & (part["p_container"] == "MED BOX")
        ]

        tmp = lineitem.merge(part, left_on="l_partkey", right_on="p_partkey")

        result = (
            tmp.groupby("p_partkey", as_index=False)
            .agg(avg_quantity=("l_quantity", "mean"))
            .assign(avg_quantity=lambda df: df["avg_quantity"] * 0.2)
            .merge(tmp, left_on="p_partkey", right_on="p_partkey")
            .pipe(lambda df: df[df["l_quantity"] < df["avg_quantity"]])
            .pipe(lambda df: df[["l_extendedprice"]].sum() / 7.0)
            .to_frame(name="avg_yearly")
        )

        return result

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
