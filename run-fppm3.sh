export RUN_LOG_TIMINGS=1
export SCALE_FACTOR=${SCALE_FACTOR:-10.0}
export RUN_IO_TYPE=skip

rm -f output/run/timings.csv
make run-fireducks
make run-fireducks
make run-fireducks
make run-polars
make run-polars
make run-polars
make run-pandas
make run-pandas
make run-pandas
make run-modin
make run-modin
make run-modin
