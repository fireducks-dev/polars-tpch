from queries.fireducks import utils

Q_NUM = 16


def q():
    utils.get_supplier_ds()
    utils.get_part_supp_ds()
    utils.get_part_ds()

    def query():
        supplier = utils.get_supplier_ds()
        partsupp = utils.get_part_supp_ds()
        part = utils.get_part_ds()

        part = part[
            (part["p_brand"] != "Brand#45")
            & (~(part["p_type"].str.startswith("MEDIUM POLISHED")))
            & part["p_size"].isin([49, 14, 23, 45, 19, 3, 36, 9])
        ]

        supplier = supplier[
            ~(supplier["s_comment"].str.contains(".*Customer.*Complaints.*"))
        ]

        result = (
            partsupp.merge(supplier, left_on="ps_suppkey", right_on="s_suppkey")
            .merge(part, left_on="ps_partkey", right_on="p_partkey")
            .groupby(["p_brand", "p_type", "p_size"], as_index=False)
            .agg(supplier_cnt=("ps_suppkey", "nunique"))
            .sort_values(
                ["supplier_cnt", "p_brand", "p_type", "p_size"],
                ascending=[False, True, True, True],
            )
        )

        return result

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
