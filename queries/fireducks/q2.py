from queries.fireducks import utils

Q_NUM = 2


def q():
    var1 = 15
    var2 = "BRASS"
    var3 = "EUROPE"

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

        part = part[(part["p_size"] == var1) & (part["p_type"].str.endswith(var2))]
        region = region[(region["r_name"] == var3)]

        merged = (
            part.merge(part_supp, left_on="p_partkey", right_on="ps_partkey")
            .merge(supplier, left_on="ps_suppkey", right_on="s_suppkey")
            .merge(nation, left_on="s_nationkey", right_on="n_nationkey")
            .merge(region, left_on="n_regionkey", right_on="r_regionkey")
        )

        final_cols = [
            "s_acctbal",
            "s_name",
            "n_name",
            "p_partkey",
            "p_mfgr",
            "s_address",
            "s_phone",
            "s_comment",
        ]

        result = (
            merged.groupby("p_partkey", as_index=False)
            .agg({"ps_supplycost": "min"})
            .merge(
                merged,
                left_on=["p_partkey", "ps_supplycost"],
                right_on=["p_partkey", "ps_supplycost"],
            )
        )[final_cols].sort_values(
            by=["s_acctbal", "n_name", "s_name", "p_partkey"],
            ascending=[False, True, True, True],
        )[:100]

        return result

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
