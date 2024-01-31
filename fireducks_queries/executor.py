import os
import shutil
import sys
from subprocess import run
from linetimer import CodeTimer


def execute_all(solution: str):
    package_name = f"{solution}_queries"
    num_queries = 22

    with CodeTimer(name=f"Overall execution of ALL {solution} queries", unit="s"):
        for i in range(1, num_queries + 1):
            run(
                [sys.executable, "-m", "fireducks.imhook", f"{package_name}/q{i}.py"],
                check=True,
            )

            if "--trace" in os.environ.get("FIREDUCKS_FLAGS", "") and os.path.isfile(
                "trace.json"
            ):
                shutil.copyfile("trace.json", f"trace_q{i:02}.json")


if __name__ == "__main__":
    execute_all("fireducks")
