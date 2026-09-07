import argparse
import pathlib

parser = argparse.ArgumentParser()
parser.add_argument("-o")
parser.add_argument("-i", required=True)
args = parser.parse_args()

pathlib.Path(args.o).parent.mkdir(parents=True, exist_ok=True)
pathlib.Path(args.o).write_text("input was: " + pathlib.Path(args.i).read_text())
