from queries.fireducks import utils

Q_NUM = 11


def q():
    utils.get_part_supp_ds()
    utils.get_supplier_ds()
    utils.get_nation_ds()

    def query():
        partsupp = utils.get_part_supp_ds()
        supplier = utils.get_supplier_ds()
        nation = utils.get_nation_ds()

        q_nation = "GERMANY"
        # https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v3.0.1.pdf
        # FRACTION is chosen as 0.0001 / SF
        # S_SUPPKEY identifier SF*10,000 are populated
        sumres_rate = 0.0001 / (supplier.shape[0] / 10000)

        result = (
            nation[nation["n_name"] == q_nation]
            .merge(supplier, left_on="n_nationkey", right_on="s_nationkey")
            .merge(partsupp, left_on="s_suppkey", right_on="ps_suppkey")
            .assign(value=lambda df: df["ps_supplycost"] * df["ps_availqty"])
            .groupby("ps_partkey", as_index=False, sort=False)
            .agg({"value": "sum"})
            .pipe(lambda df: df[df["value"] > df["value"].sum() * sumres_rate])
            .sort_values(by=["value", "ps_partkey"], ascending=[False, True])
            .reset_index(drop=True)
        )

        return result

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
