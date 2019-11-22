#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import io
import os
import traceback
import argparse
from datetime import datetime, MINYEAR, MAXYEAR
import glob
import pandas as pd
import numpy as np



years = list(range(2005,2012))
months = list(range(1,13))

dates = ["%d-%02d"%(y,m) for y in years for m in months]



def get_path(base, year, month):
	return os.path.join(base, "%d-%d"%(year, month))


def get_id_df(path):
	fn = os.path.join(path, "all_ids.csv")
	return pd.read_csv(fn, header=None, names=["id_str", "id"])





parser = argparse.ArgumentParser(description="Look for id overlaps")
parser.add_argument('data', help='data directory')

args = parser.parse_args()

left_name = os.path.basename(args.data)


ids = set()

for d in dates:

	data_dir = os.path.join(args.data, d)

	print data_dir

	if os.path.exists(data_dir):
		df = pd.read_csv(
			os.path.join(data_dir, "all_ids.csv"),
			header=None,
			names=["id_str", "id"])

		new_ids_list = list(df['id_str'])
		new_ids = set(new_ids_list)

		if len(new_ids_list) != len(new_ids):
			print "    > inner collision!"

		overlaps = ids & new_ids
		if len(overlaps) > 0:
			print "    > collisions found (%):" % (len(overlaps))
			print "".join(["\n    > %s"%(o) for o in overlaps])

		ids.update(new_ids_list)

