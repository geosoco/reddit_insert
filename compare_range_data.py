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



years = list(range(2006,2011))
months = list(range(1,13))

dates = ["%d-%02d"%(y,m) for y in years for m in months]



def get_path(base, year, month):
	return os.path.join(base, "%d-%d"%(year, month))


def get_id_df(path):
	fn = os.path.join(path, "all_ids.csv")
	return pd.read_csv(fn, header=None, names=["id_str", "id"])



def compare_ids(lpath, rpath):
	ldf = get_id_df(lpath)
	rdf = get_id_df(rpath)

	sl = set(ldf['id_str'])
	sr = set(rdf['id_str'])

	print "in common:     ", len(sl & sr)
	print "left exclusive ", len(sl - sr)
	print "right exclusive", len(sr - sl)


	return {
		'left_total': len(ldf['id_str']),
		'left_unique': len(sl),
		'left_exclusive': len(sl-sr),
		'right_total': len(rdf['id_str']),
		'right_unique': len(sr),
		'right_exclusive': len(sr-sl),
		'in_common': len(sl & sr)
	}



parser = argparse.ArgumentParser(description="dataset id comparison")
parser.add_argument('left', help='first directory')
parser.add_argument('right', help='second directory')

args = parser.parse_args()


left_name = os.path.basename(args.left)
right_name = os.path.basename(args.right)


print "Left:", left_name, "\tright:", right_name


df = pd.DataFrame(data={left_name: np.zeros(len(dates)), right_name: np.zeros(len(dates))}, index=dates)


for d in dates:
	left = os.path.join(args.left, d)
	right = os.path.join(args.right, d)

	if os.path.exists(left):
		df.loc[d, left_name] = 1

	if os.path.exists(right):
		df.loc[d, right_name] = 1


print "Date comparison for datasets"

print df.loc[df[left_name] != df[right_name]]

print "-" * 70, "\n" * 2


matchings_months = list(df.index[df[left_name] == df[right_name]])


df = pd.DataFrame(data={
		date: dates,
		"v1": np.zeroes(len(dates)),
		"v2": np.zeroes(len(dates)),

	})

for d in matchings_months:
	left = os.path.join(args.left, d)
	right = os.path.join(args.right, d)

	#print d
	#print "-" * 40

	_cmp = compare_ids(left, right)
	_cmp['date'] = d

	