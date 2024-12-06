export RUN_LOG_TIMINGS=1
export SCALE_FACTOR=${SCALE_FACTOR:-10.0}
export RUN_IO_TYPE=skip

make tables-pyarrow
make run-fireducks
