#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import bz2
import os

parser = argparse.ArgumentParser(description="reddit json importer")
parser.add_argument('filename', help='filename to import')


CHUNK_SIZE = 64 * 1024

args = parser.parse_args()

print "Opening %s and saving to %s" % ( args.filename, os.path.splitext(args.filename)[0])

with bz2.BZ2File(args.filename, "rb", 1024*1024*8) as infile:
	with open(os.path.splitext(args.filename)[0], "wb") as outfile:
		while True:
			data = infile.read(CHUNK_SIZE)
			if len(data) == 0:
				break
			outfile.write(data)

