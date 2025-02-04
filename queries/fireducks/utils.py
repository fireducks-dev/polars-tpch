import timeit
from os.path import join
from typing import Callable, Any

import pandas as pd
from linetimer import CodeTimer, linetimer
from pandas.core.frame import DataFrame as PandasDF

from queries.common_utils import (
    # _get_query_answer_pd,
    check_query_result_pd,
    get_table_path,
    log_query_timing,
    on_second_call,
)
from settings import Settings

settings = Settings()


import sys

fd = sys.modules.get("fireducks")
if fd is not None:
    # to get the actual FireDucks version
    fd.core.set_fireducks_option("fireducks-version", True)


def _read_ds(table_name: str) -> PandasDF:
    path = get_table_path(table_name)
    if settings.run.io_type in ("parquet", "skip"):
        # Workaround: dtype_backend="pyarrow" is not supported by pandas-1.5.3
        df = pd.read_parquet(path)
    elif settings.run.io_type == "feather":
        return pd.read_feather(path)
    else:
        raise ValueError(f"file type: {settings.run.io_type} not expected")

    if not settings.run.include_io:
        if hasattr(df, "_evaluate"):
            df._evaluate()
    return df


def get_query_answer(query: int) -> PandasDF:
    path = settings.paths.answers / f"q{query}.parquet"
    return pd.read_parquet(path).to_pandas()


def test_results(q_num: int, result: PandasDF):
    from pandas.testing import assert_frame_equal

    expected = get_query_answer(q_num)
    for col in expected.columns:
        if "dat" in col:
            expected[col] = expected[col].astype(result[col].dtype)

    assert_frame_equal(result.reset_index(drop=True), expected, check_dtype=False)


def on_second_call_if_excluding_io(func: Any) -> Any:
    def helper(*args: Any, **kwargs: Any) -> Any:
        if not settings.run.include_io:
            helper.calls += 1  # type: ignore[attr-defined]

            # first call is outside the function
            # this call must set the result
            if helper.calls == 1:  # type: ignore[attr-defined]
                helper.result = func(*args, **kwargs)  # type: ignore[attr-defined]
                return helper.result  # type: ignore[attr-defined]
            return helper.result  # type: ignore[attr-defined]
        else:
            return func(*args, **kwargs)

    helper.calls = 0  # type: ignore[attr-defined]
    helper.result = None  # type: ignore[attr-defined]
    return helper


@on_second_call_if_excluding_io
def get_line_item_ds() -> pd.DataFrame:
    return _read_ds("lineitem")


@on_second_call_if_excluding_io
def get_orders_ds() -> pd.DataFrame:
    return _read_ds("orders")


@on_second_call_if_excluding_io
def get_customer_ds() -> pd.DataFrame:
    return _read_ds("customer")


@on_second_call_if_excluding_io
def get_region_ds() -> pd.DataFrame:
    return _read_ds("region")


@on_second_call_if_excluding_io
def get_nation_ds() -> pd.DataFrame:
    return _read_ds("nation")


@on_second_call_if_excluding_io
def get_supplier_ds() -> pd.DataFrame:
    return _read_ds("supplier")


@on_second_call_if_excluding_io
def get_part_ds() -> pd.DataFrame:
    return _read_ds("part")


@on_second_call_if_excluding_io
def get_part_supp_ds() -> pd.DataFrame:
    return _read_ds("partsupp")


def run_query(query_number: int, query: Callable):
    def run():
        with CodeTimer(name=f"Run fireducks query {query_number}", unit="s") as timer:
            result = query()
            if hasattr(result, "_evaluate"):
                result._evaluate()

        if settings.run.log_timings:
            log_query_timing(
                solution="fireducks",
                version=pd.__version__,
                query_number=query_number,
                time=timer.took,
            )

        if settings.run.check_results:
            if hasattr(result, "to_pandas"):
                result = result.to_pandas()
            test_results(query_number, result)

        if settings.run.show_results:
            print(result)

    run()
