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

from status_updater import *
from collections import defaultdict

#
# PARSER
#

parser = argparse.ArgumentParser(description="reddit json importer")
parser.add_argument('filename', help='filename to import')
parser.add_argument('--filename2')
parser.add_argument('--outfile')
parser.add_argument('-u', '--user', help="username")
parser.add_argument('-p', '--password', help="password", action="store_true")
parser.add_argument('--live_id', help="id of live thread")


args = parser.parse_args()




inserted_ids = defaultdict(int)


files = [args.filename]
if args.filename2 is not None and len(args.filename2) > 0:
    files.append(args.filename2)


for filename in files:

    mo = re.match(r"R[SC]_(\d{4})-(\d{2}).*", os.path.basename(filename), re.I)
    if mo is None:
        print "Couldn't parse date from filename. (expects R[SC]_YYYY-MM.*)"
        print os.path.basename(filename)
        quit(1)


    #print "filename matches: ", mo.group(1), mo.group(2)

    status_updater = StatusUpdater()
    status_updater.total_files = 1

    table_dt = datetime(int(mo.group(1)), int(mo.group(2)), 1)
    next_table_dt = table_dt + relativedelta(months=1)




    if filename.endswith(".bz2"):
        print "detected bz2 file..."
        infile = bz2.BZ2File(filename, "r", 1024*1024*64)
        file_length = 0
    else:
        infile = open(filename, "r")

        # get file length
        infile.seek(0, os.SEEK_END)
        file_length = infile.tell()
        infile.seek(0, os.SEEK_SET)


    try:


        # Core loop
        while True:
            lines = [json.loads(i) for i in islice(infile, 64)]

            for l in lines:
                print json.dumps(l, indent=4)
                id = int(l["id"], 36)
                created_utc_s = int(l["created_utc"])
                dt = datetime.utcfromtimestamp(created_utc_s)

                if id in inserted_ids:
                    inserted_ids[id] += 1
                    print "duplicate!", l["id"],  dt, inserted_ids[id]
                    #print "\n\n<<<<< Old"
                    #print json.dumps(inserted_ids[id], indent=4)
                    #print "\n\n>>>>> New "
                    #print json.dumps(l, indent=4)
                else:
                    inserted_ids[id] = 1


                if dt < table_dt or dt >= next_table_dt:
                    print "INVALID DATE!!!"
                    print "(%s, %s) : %s" % (
                        table_dt, dt, next_table_dt)
                    print created_utc_s


            # enough is enough
            if lines is None or len(lines) == 0:
                break

            quit()

    finally:
        # make sure to close the file
        infile.close()


status_updater.update(force=True)
print "Completed successfully!"


outfile = None
if args.outfile is not None:
    outfile = open(args.outfile, "w")
else:
    outfile = sys.stdout

print "total dupes: ", len(inserted_ids)
for id,cnt in inserted_ids.iteritems():
    if cnt > 1:
        outfile.write("%d,%d" % (id, cnt))

if args.outfile is not None:
    outfile.close()
            
