import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-o")
parser.add_argument("-i", default=None)
parser.parse_args()
# deliberately does not write the output file
