from queries.modin import utils

Q_NUM = 17


def q():
    utils.get_line_item_ds()
    utils.get_part_ds()

    def query():
        lineitem = utils.get_line_item_ds()
        part = utils.get_part_ds()

        var1 = "Brand#23"
        var2 = "MED BOX"

        q1 = (
            part[part["p_brand"] == var1]
            .pipe(lambda df: df[df["p_container"] == var2])
            .merge(lineitem, left_on="p_partkey", right_on="l_partkey")
        )

        q_final = (
            q1.groupby("p_partkey", as_index=False)
            .agg(avg_quantity=("l_quantity", "mean"))
            .assign(avg_quantity=lambda df: df["avg_quantity"] * 0.2)
            .merge(q1, left_on="p_partkey", right_on="p_partkey")
            .pipe(lambda df: df[df["l_quantity"] < df["avg_quantity"]])
            .pipe(lambda df: df[["l_extendedprice"]].sum() / 7.0)
            .to_frame(name="avg_yearly")
        )

        return q_final

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
