from queries.pandas import utils

Q_NUM = 16


def q():
    utils.get_supplier_ds()
    utils.get_part_supp_ds()
    utils.get_part_ds()

    def query():
        supplier = utils.get_supplier_ds()
        partsupp = utils.get_part_supp_ds()
        part = utils.get_part_ds()

        var1 = "Brand#45"

        supplier = supplier[
            supplier["s_comment"].str.contains(".*Customer.*Complaints.*")
        ][["s_suppkey"]]

        q_final = (
            part.merge(partsupp, left_on="p_partkey", right_on="ps_partkey")
            .pipe(lambda df: df[df["p_brand"] != var1])
            .pipe(lambda df: df[~(df["p_type"].str.startswith("MEDIUM POLISHED"))])
            .pipe(lambda df: df[df["p_size"].isin([49, 14, 23, 45, 19, 3, 36, 9])])
            .merge(supplier, left_on="ps_suppkey", right_on="s_suppkey", how="left")
            .pipe(lambda df: df[df["s_suppkey"].isnull()])
            .groupby(["p_brand", "p_type", "p_size"], as_index=False)
            .agg(supplier_cnt=("ps_suppkey", "nunique"))
            .sort_values(
                ["supplier_cnt", "p_brand", "p_type", "p_size"],
                ascending=[False, True, True, True],
            )
        )

        return q_final

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
