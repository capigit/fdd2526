from pathlib import Path
import runpy


if __name__ == "__main__":
    runpy.run_path(Path(__file__).with_name("fusion_bases_v2.py"), run_name="__main__")
