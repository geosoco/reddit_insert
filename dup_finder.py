#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import io
import sys
import os
import traceback
import argparse
import getpass
from datetime import datetime
import bz2
import re
import ujson as json
from itertools import islice
from dateutil.relativedelta import relativedelta
import time
import calendar

from utils.file_utils import is_pipe, set_pipe_size

from status_updater import *
from collections import defaultdict

#
# PARSER
#

parser = argparse.ArgumentParser(description="check file for duplicate ids")
parser.add_argument('filename', help='filename to check')
parser.add_argument('--filename2', help='additional file to check')
parser.add_argument('--outfile', help='csv filename to write output to (default: stdout)')

args = parser.parse_args()




inserted_ids = defaultdict(int)
dups = {}


files = [args.filename]
if args.filename2 is not None and len(args.filename2) > 0:
    files.append(args.filename2)


status_updater = StatusUpdater()
status_updater.total_files = len(files)

for filename in files:

    # reset the count
    status_updater.count = 0

    # open file
    if filename.endswith(".bz2"):
        print("detected bz2 file...")
        infile = bz2.BZ2File(filename, "r")
        file_length = 0
    else:
        infile = open(filename, "r")

        if is_pipe(filename) is False:
            # get file length
            infile.seek(0, os.SEEK_END)
            file_length = infile.tell()
            infile.seek(0, os.SEEK_SET)
        else:
            set_pipe_size(infile, 1024*1024)

    last_id = 0

    try:

        for idx, line in enumerate(infile):
            l = json.loads(line)
            id = int(l["id"], 36)

            if id < last_id:
                print("out of order ids. (this: {}, last: {})".format(id, last_id))

            last_id = id

            #print json.dumps(l, indent=4)
            #quit()
            if id in inserted_ids:
                inserted_ids[id] += 1
                dups[id] = l
                print("duplicate!", l["id"], inserted_ids[id], idx)
                #print "\n\n<<<<< Old"
                #print json.dumps(inserted_ids[id], indent=4)
                #print "\n\n>>>>> New "
                #print json.dumps(l, indent=4)
            else:
                inserted_ids[id] = 1


            status_updater.count += 1
            status_updater.total_added += 1
            status_updater.update()

        status_updater.current_file += 1
        status_updater.update(force=True)

    finally:
        # make sure to close the file
        infile.close()


status_updater.update(force=True)
print("Completed successfully!")


outfile = None
if args.outfile is not None:
    outfile = open(args.outfile, "w")
else:
    outfile = sys.stdout

print("total ids: ", len(inserted_ids))
print("total duplicates: ", len(dups))
for id,cnt in inserted_ids.items():
    if cnt > 1:
        l = dups[id]
        created_utc_s = int(l["created_utc"])
        dt = datetime.utcfromtimestamp(created_utc_s)
        outfile.write("%d,%d,%s\n" % (id, cnt, dt))

if args.outfile is not None:
    outfile.close()
            
