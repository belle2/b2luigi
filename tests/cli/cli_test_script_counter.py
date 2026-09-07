"""Fixture script: increments counter.txt and writes the -o output file.

Used to detect whether the script actually ran (counter increases) vs.
Luigi skipped the task (counter unchanged).
"""
import argparse
import pathlib

parser = argparse.ArgumentParser()
parser.add_argument("-o")
args = parser.parse_args()

counter_file = pathlib.Path("counter.txt")
count = int(counter_file.read_text()) if counter_file.exists() else 0
counter_file.write_text(str(count + 1))

pathlib.Path(args.o).parent.mkdir(parents=True, exist_ok=True)
pathlib.Path(args.o).write_text("done")
