export FIREDUCKS_FLAGS="--fireducks-version"

export RUN_IO_TYPE="skip"
export RUN_LOG_TIMINGS=1
export SCALE_FACTOR=1.0

echo run with cached IO
make tables
make run-all
make plot

echo run with IO
export RUN_INCLUDE_IO=1
make run-all
make plot
