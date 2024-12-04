polars-tpch with FireDucks
==========================

This repo contains the code used for performance evaluation of FireDucks. The benchmarks are based on https://github.com/pola-rs/tpch, and queries for FireDucks are added.

See the original README [here](README_original.md).

## Instructions

```
# install required packages
$ sudo apt update
$ sudo apt install python3.10-venv make gcc

# clone benchmark
$ git clone https://github.com/fireducks-dev/polars-tpch
$ cd polars-tpch

# prepare venv for fireducks
$ python -mvenv fireducks-venv
$ fireducks-venv/bin/pip install fireducks linetimer pydantic pydantic_settings

# prepare dataset by pyarrow
$ make -C tpch-dbgen dbgen
$ (cd tpch-dbgen && ./dbgen -vf -s 10)
$ (mkdir -p data/tables_pyarrow/scale-10.0 && mv tpch-dbgen/*.tbl data/tables_pyarrow/scale-10.0/)
$ PATH_TABLES=data/tables_pyarrow SCALE_FACTOR=10 ./fireducks-venv/bin/python -m scripts.prepare_data_pyarrow
$ rm data/tables_pyarrow/scale-10.0/*.tbl # to save disk space

# run with fireducks
$ PATH_TABLES=data/tables_pyarrow SCALE_FACTOR=10 RUN_IO_TYPE=skip RUN_LOG_TIMINGS=True fireducks-venv/bin/python -m queries.fireducks

# you will see all timings in `output/run/timings.csv`
```
