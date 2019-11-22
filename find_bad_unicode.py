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
import re

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
parser.add_argument('--live_id', help="id of live thread")


args = parser.parse_args()



re_null =re.compile("\x00")



inserted_ids = defaultdict(int)


files = [args.filename]
if args.filename2 is not None and len(args.filename2) > 0:
    files.append(args.filename2)


for filename in files:

    status_updater = StatusUpdater()
    status_updater.total_files = 1



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
            lines = [i for i in islice(infile, 64)]

            for l in lines:
                ll = l.lower() if l is not None else l
                if re_null.search(ll) is not None:
                    print "\n"* 4, "found null"
                    print l
                    print "-"*70

                    print json.dumps(l, indent=4)
                    print "="*70


                obj = json.loads(l)
                author = obj.get('author', None)
                sub = obj.get('subreddit', "")
                timestamp = int(obj["created_utc"])
                id = obj.get('id', "<<bad>>")
                dt = datetime.utcfromtimestamp(timestamp)

                if 'ineWhite143' in author and dt.day == 9 and dt.hour == 12:
                    print "found one..."
                    print l, "\n\n"
                    print json.dumps(obj, indent=4)
                    of_fn = "bad_unicode_{}.json".format(id)
                    of = open(of_fn, "w")
                    of.write(l + "\n")
                    of.close()


                status_updater.count += 1
                status_updater.total_added += 1
                status_updater.update()


            # enough is enough
            if lines is None or len(lines) == 0:
                break



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


if args.outfile is not None:
    outfile.close()
            
