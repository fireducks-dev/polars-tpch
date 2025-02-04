#! /bin/sh

set -xe

export RUN_LOG_TIMINGS=1
export SCALE_FACTOR=${SCALE_FACTOR:-10.0}
export RUN_IO_TYPE=${RUN_IO_TYPE:-skip}
PATH_TIMINGS_FILENAME=${PATH_TIMINGS_FILENAME:-timings.csv}

VENV_FIREDUCKS=.venv-fireducks
VENV=.venv

# make data/tables_pyarrow_large where comment columns are large_string.
# pandas requires this dataset to avoid segv.
if test ! -d data/tables_pyarrow_large/scale-${SCALE_FACTOR}; then
	LARGE_STRING_COMMENT=True PATH_TABLES=data/tables_pyarrow_large make tables-pyarrow
fi

# rm -f output/run/${PATH_TIMINGS_FILENAME}

export PATH_TABLES=data/tables_pyarrow
${VENV_FIREDUCKS}/bin/python -m queries.fireducks
${VENV_FIREDUCKS}/bin/python -m queries.fireducks
${VENV_FIREDUCKS}/bin/python -m queries.fireducks

# use data/tables_pyarrow for better performance
export PATH_TABLES=data/tables_pyarrow
${VENV}/bin/python -m queries.polars
${VENV}/bin/python -m queries.polars
${VENV}/bin/python -m queries.polars

export PATH_TABLES=data/tables_pyarrow
${VENV}/bin/python -m queries.duckdb
${VENV}/bin/python -m queries.duckdb
${VENV}/bin/python -m queries.duckdb

export PATH_TABLES=data/tables_pyarrow_large
${VENV}/bin/python -m queries.pandas
${VENV}/bin/python -m queries.pandas
${VENV}/bin/python -m queries.pandas
