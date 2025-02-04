export RUN_LOG_TIMINGS=1
export SCALE_FACTOR=${SCALE_FACTOR:-10.0}
export RUN_IO_TYPE=skip

python3 -mvenv .venv
.venv/bin/pip install pyarrow pydantic pydantic_settings linetimer

PATH_TABLES=data/tables_pyarrow make tables-pyarrow
make run-fireducks
