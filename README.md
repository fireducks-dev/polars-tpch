polars-tpch with FireDucks
==========================

This repo contains the code used for performance evaluation of FireDucks. The benchmarks are based on https://github.com/pola-rs/tpch, and queries for FireDucks are added.

You can find the original README [here](README_original.md).

## Instructions

```
# clone this repository
$ git clone https://github.com/fireducks-dev/polars-tpch
$ cd polars-tpch

# Run
$ SCALE_FACTOR=10.0 ./run-fireducks.sh
```

You will see all timings in `output/run/timings.csv`


To run fireducks, polars, pandas and modin three times:

```
$ SCALE_FACTOR=10.0 make tables
$ .venv/bin/pip install -U pandas polars modin
$ ./run-fppm3.sh
```
