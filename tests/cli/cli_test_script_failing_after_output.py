import argparse
import pathlib
import sys

parser = argparse.ArgumentParser()
parser.add_argument("-o")
args = parser.parse_args()

pathlib.Path(args.o).parent.mkdir(parents=True, exist_ok=True)
pathlib.Path(args.o).write_text("done")
sys.exit(42)
