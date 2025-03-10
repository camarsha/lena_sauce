import sauce
import subprocess
from pathlib import Path
from . import detector_setup


def make_midas_name(number, suffix="mid.lz4"):
    temp = str(number).rjust(5, "0")
    return f"run{temp}.{suffix}"


def load_midas_run(filename):
    filename = Path(filename)
    print(str(filename))
    dir = filename.parent
    midas_file = filename.stem.split(".")[0]
    parquet_file = midas_file + ".parquet"
    if not (dir / parquet_file).exists():
        subprocess.run(
            [
                "midas-converter",
                "-p",
                str(filename),
                "/home/caleb/midas-data/LENA-data/mdpp-tests/mdpp-setup.toml",
                "--chunk-size 1000000",
            ]
        )
    return sauce.Run(str(dir / parquet_file))
