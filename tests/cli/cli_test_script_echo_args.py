"""Fixture script: writes sys.argv to the -o output file.

Used to verify that extra args are forwarded correctly to the subprocess.
"""
import argparse
import pathlib
import sys

parser = argparse.ArgumentParser()
parser.add_argument("-o")
parser.add_argument("-i", default=None)
args, unknown = parser.parse_known_args()

pathlib.Path(args.o).parent.mkdir(parents=True, exist_ok=True)
pathlib.Path(args.o).write_text(" ".join(sys.argv))
