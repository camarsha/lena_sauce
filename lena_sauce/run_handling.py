import sauce
import subprocess
from pathlib import Path
from . import detector_setup
import importlib.resources

mdpp_setup = importlib.resources.files("lena_sauce") / "mdpp-setup.toml"


def make_midas_name(number, suffix="mid.lz4"):
    temp = str(number).rjust(5, "0")
    return f"run{temp}.{suffix}"


def load_midas_run(filename, mdpp_setup=mdpp_setup, chunk_size=1000000):
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
                str(mdpp_setup),
                "--chunk-size",
                str(chunk_size),
            ]
        )
    return sauce.Run(str(dir / parquet_file))
