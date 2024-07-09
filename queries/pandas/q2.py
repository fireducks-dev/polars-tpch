from queries.pandas import utils

Q_NUM = 2


def q():
    # first call one time to cache in case we don't include the IO times
    utils.get_region_ds()
    utils.get_nation_ds()
    utils.get_supplier_ds()
    utils.get_part_ds()
    utils.get_part_supp_ds()

    def query():
        region = utils.get_region_ds()
        nation = utils.get_nation_ds()
        supplier = utils.get_supplier_ds()
        part = utils.get_part_ds()
        part_supp = utils.get_part_supp_ds()

        var1 = 15
        var2 = "BRASS"
        var3 = "EUROPE"

        q1 = (
            part.merge(part_supp, left_on="p_partkey", right_on="ps_partkey")
            .merge(supplier, left_on="ps_suppkey", right_on="s_suppkey")
            .merge(nation, left_on="s_nationkey", right_on="n_nationkey")
            .merge(region, left_on="n_regionkey", right_on="r_regionkey")
            .pipe(lambda df: df[df["p_size"] == var1])
            .pipe(lambda df: df[df["p_type"].str.endswith(var2)])
            .pipe(lambda df: df[df["r_name"] == var3])
        )

        q_final = (
            (
                q1.groupby("p_partkey", as_index=False)
                .agg({"ps_supplycost": "min"})
                .merge(q1, on=["p_partkey", "ps_supplycost"])
            )[
                [
                    "s_acctbal",
                    "s_name",
                    "n_name",
                    "p_partkey",
                    "p_mfgr",
                    "s_address",
                    "s_phone",
                    "s_comment",
                ]
            ]
            .sort_values(
                by=["s_acctbal", "n_name", "s_name", "p_partkey"],
                ascending=[False, True, True, True],
            )
            .head(100)
        )

        return q_final

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
