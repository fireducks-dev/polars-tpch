import sys

import pyarrow as pa
import pyarrow.types
import pyarrow.csv
import pyarrow.parquet

# scale_fac = int(sys.argv[1])
input_dir = sys.argv[1]
output_dir = sys.argv[2]

h_nation = """n_nationkey
n_name
n_regionkey
n_comment
n_dummy""".split("\n")

h_region = """r_regionkey
r_name
r_comment
r_dummy""".split("\n")

h_part = """p_partkey
p_name
p_mfgr
p_brand
p_type
p_size
p_container
p_retailprice
p_comment
p_dummy""".split("\n")

h_supplier = """s_suppkey
s_name
s_address
s_nationkey
s_phone
s_acctbal
s_comment
s_dummy""".split("\n")

h_partsupp = """ps_partkey
ps_suppkey
ps_availqty
ps_supplycost
ps_comment
ps_dummy""".split("\n")

h_customer = """c_custkey
c_name
c_address
c_nationkey
c_phone
c_acctbal
c_mktsegment
c_comment
c_dummy""".split("\n")

h_orders = """o_orderkey
o_custkey
o_orderstatus
o_totalprice
o_orderdate
o_orderpriority
o_clerk
o_shippriority
o_comment
o_dummy""".split("\n")

h_lineitem = """l_orderkey
l_partkey
l_suppkey
l_linenumber
l_quantity
l_extendedprice
l_discount
l_tax
l_returnflag
l_linestatus
l_shipdate
l_commitdate
l_receiptdate
l_shipinstruct
l_shipmode
l_comments
l_dummy""".split("\n")


def cast_date(tbl):
    fields = []
    for field in tbl.schema:
        print(f"{field.name} {field.type} {pa.types.is_date32(field.type)}")
        if pa.types.is_date32(field.type):
            fields += [pa.field(field.name, pa.timestamp("ns"))]
        else:
            fields += [field]

    schema = pa.schema(fields)
    print(schema)
    return tbl.cast(schema)


for name in [
        "nation",
        "region",
        "part",
        "supplier",
        "partsupp",
        "customer",
        "orders",
        "lineitem",
]:
    print("process table:", name)
    tbl = pa.csv.read_csv(
        f"{input_dir}/{name}.tbl",
        parse_options=pa.csv.ParseOptions(delimiter="|"),
        read_options=pa.csv.ReadOptions(column_names=eval(f"h_{name}")))
    tbl = cast_date(tbl)
    path = f"{output_dir}/{name}.parquet"
    pa.parquet.write_table(tbl, path)
