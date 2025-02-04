import os
import shutil
import sys
from subprocess import run
from linetimer import CodeTimer

from queries.common_utils import _get_query_numbers
from settings import Settings


settings = Settings()


def execute_all(library_name: str):
    print(settings.model_dump_json())

    query_numbers = _get_query_numbers(library_name)
    # query_numbers = [10]

    with CodeTimer(name=f"Overall execution of ALL {library_name} queries", unit="s"):
        for i in query_numbers:
            run(
                [
                    sys.executable,
                    "-m",
                    "fireducks.imhook",
                    f"queries/{library_name}/q{i}.py",
                ],
                check=True,
            )

            if "--trace" in os.environ.get("FIREDUCKS_FLAGS", "") and os.path.isfile(
                "trace.json"
            ):
                shutil.copyfile("trace.json", f"trace_q{i:02}.json")


if __name__ == "__main__":
    execute_all("fireducks")
