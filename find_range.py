#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import io
import os
import traceback
import argparse
import getpass
from datetime import datetime, MINYEAR, MAXYEAR
import bz2
import re
#import ujson as json
import json
from itertools import islice
from dateutil.relativedelta import relativedelta
import time
import calendar
import glob
import string
import copy
from collections import defaultdict


from status_updater import *



def grouper(n, iterable):
    it = iter(iterable)
    while True:
        chunk = tuple(islice(it, n))
        if not chunk:
            return
        yield chunk

def hex_view(m):
    return ' '.join([hex(ord(b))[2:].zfill(2) for b in m])

def char_view(m):
    return ''.join([b if ord(b) >= 32 and ord(b) < 126 else '.' for b in m])

def hexdump(m, n=16):
    print('\n'.join([hex_view(a).ljust(n*2+n-1) + "  " + char_view(a) for a in grouper(n,m)]))




min_date = datetime(MAXYEAR, 1, 1)
max_date = datetime(MINYEAR, 1, 1)
min_id = 999999999999999
max_id = -1

default_date_dict = {
    "date_min": min_date,
    "date_max": max_date,
    "file_date_min": min_date,
    "file_date_max": max_date,
    "oob_date_min": min_date,
    "oob_date_max": max_date,
    "id_min": min_id,
    "id_max": max_id,
    "oob_id_min": min_id,
    "oob_id_max": max_id,
    "file_id_min": min_id,
    "file_id_max": max_id,
    "file_oob_dates": 0,
    "file_missing_authors": 0,
    "file_missing_subs": 0,
    "file_missing_articles": 0
}



class range_dict(object):

    def __init__(self):
        self.years = defaultdict(lambda: defaultdict(lambda: copy.deepcopy(default_date_dict)))
        self.cur_month = -1
        self.cur_year = -1

    def set_default_date(self, year, month):
        self.cur_year = year
        self.cur_month = month


    def add_date(self, dt):
        if dt.year == self.cur_year and dt.month == self.cur_month:
            # in bounds
            self.years[dt.year][dt.month]["date_min"] = min(self.years[dt.year][dt.month]["date_min"], dt)
            self.years[dt.year][dt.month]["date_max"] = max(self.years[dt.year][dt.month]["date_max"], dt)
        else:
            # out of bounds
            #self.years[self.cur_year][self.cur_month]["date_min"] = min(self.years[dt.year][dt.month]["date_min"], dt)
            #self.years[self.cur_year][self.cur_month]["date_max"] = min(self.years[dt.year][dt.month]["date_max"], dt)
            self.years[dt.year][dt.month]["oob_date_min"] = min(self.years[dt.year][dt.month]["oob_date_min"], dt)
            self.years[dt.year][dt.month]["oob_date_max"] = max(self.years[dt.year][dt.month]["oob_date_max"], dt)

        self.years[self.cur_year][self.cur_month]["file_date_min"] = min(self.years[dt.year][dt.month]["file_date_min"], dt)
        self.years[self.cur_year][self.cur_month]["file_date_max"] = max(self.years[dt.year][dt.month]["file_date_max"], dt)


    def add_date_and_id(self, dt, id):
        if dt.year == self.cur_year and dt.month == self.cur_month:
            # in bounds
            self.years[dt.year][dt.month]["date_min"] = min(self.years[dt.year][dt.month]["date_min"], dt)
            self.years[dt.year][dt.month]["date_max"] = max(self.years[dt.year][dt.month]["date_max"], dt)
            self.years[dt.year][dt.month]["id_min"] = min(self.years[dt.year][dt.month]["id_min"], id)
            self.years[dt.year][dt.month]["id_max"] = max(self.years[dt.year][dt.month]["id_max"], id)
        else:
            # out of bounds
            self.years[self.cur_year][self.cur_month]["oob_date_min"] = min(self.years[self.cur_year][self.cur_month]["oob_date_min"], dt)
            self.years[self.cur_year][self.cur_month]["oob_date_max"] = max(self.years[self.cur_year][self.cur_month]["oob_date_max"], dt)
            self.years[self.cur_year][self.cur_month]["oob_id_min"] = min(self.years[self.cur_year][self.cur_month]["oob_id_min"], id)
            self.years[self.cur_year][self.cur_month]["oob_id_max"] = max(self.years[self.cur_year][self.cur_month]["oob_id_max"], id)

            self.years[dt.year][dt.month]["date_min"] = min(self.years[dt.year][dt.month]["date_min"], dt)
            self.years[dt.year][dt.month]["date_max"] = max(self.years[dt.year][dt.month]["date_max"], dt)
            self.years[dt.year][dt.month]["id_min"] = min(self.years[dt.year][dt.month]["id_min"], id)
            self.years[dt.year][dt.month]["id_max"] = max(self.years[dt.year][dt.month]["id_max"], id)


            self.years[self.cur_year][self.cur_month]["file_oob_dates"] += 1


        self.years[self.cur_year][self.cur_month]["file_date_min"] = min(self.years[dt.year][dt.month]["file_date_min"], dt)
        self.years[self.cur_year][self.cur_month]["file_date_max"] = max(self.years[dt.year][dt.month]["file_date_max"], dt)
        self.years[self.cur_year][self.cur_month]["file_id_min"] = min(self.years[dt.year][dt.month]["file_id_min"], id)
        self.years[self.cur_year][self.cur_month]["file_id_max"] = max(self.years[dt.year][dt.month]["file_id_max"], id)



#
# PARSER
#

parser = argparse.ArgumentParser(description="reddit json importer")
parser.add_argument('filename', help='filename to import')
parser.add_argument('--filename2')
parser.add_argument('-u', '--user', help="username")
parser.add_argument('-p', '--password', help="password", action="store_true")
parser.add_argument('--live_id', help="id of live thread")
parser.add_argument('--outdir', help="optional output directory for missing data files")


args = parser.parse_args()




ranges_dict = range_dict()
inserted_ids = {}


files = glob.glob(args.filename)
if args.filename2 is not None and len(args.filename2) > 0:
    files.append(args.filename2)

status_updater = StatusUpdater()
status_updater.total_files = len(files)

print len(files)



my_whitespace = string.whitespace + "\0"


for filename in files:

    basename = os.path.basename(filename)
    basename_no_ext = os.path.splitext(basename)[0]

    #print basename, basename_no_ext

    mo = re.match(r"R[SC]_(v2_)?(\d{4})-(\d{2}).*", basename, re.I)
    if mo is None:
        print "Couldn't parse date from filename. (expects R[SC]_YYYY-MM.*)"
        print os.path.basename(filename)
        quit(1)


    #print "filename matches: ", mo.group(1), mo.group(2)

    year = int(mo.group(2))
    month = int(mo.group(3))

    comment = os.path.basename(filename).startswith("RC")


    print year, month


    ranges_dict.set_default_date(year, month)


    min_date = datetime(MAXYEAR, 1, 1)
    max_date = datetime(MINYEAR, 1, 1)
    min_id = sys.maxint
    max_id = -1


    if filename.endswith(".bz2"):
        #print "detected bz2 file..."
        infile = bz2.BZ2File(filename, "r", 1024*1024*8)
        file_length = 0
    else:
        infile = open(filename, "r")

        # get file length
        infile.seek(0, os.SEEK_END)
        file_length = infile.tell()
        infile.seek(0, os.SEEK_SET)



    missing_authors = 0
    missing_subs = 0
    missing_articles = 0


    oob_date_items = []
    missing_data_ids = set()
    all_ids = set()
    dupcliate_ids = []

    dt = None
    last_dt = None

    # Core loop
    for line in infile:
        line = line.strip(my_whitespace)
        if "\\u0000" in line or "\u0000" in line or "\0" in line:
            print "File contains null sequence"
            print line
            line = line.replace("\\u0000", "\\\\u0000")
            line = line.replace("\u0000", "\\\\u0000")
            line = line.replace("\0", "<<::NULL::>>")

        if line is None or len(line) < 1:
            continue

        try:
            obj = json.loads(line)
        except ValueError, e:
            print "Couldn't read line"
            print "-"*50
            print line
            print "-"*50
            hexdump(line)

        id = obj["id"]
        if id in all_ids:
            dupcliate_ids.append(id)

        all_ids.add(id)

        timestamp = int(obj["created_utc"])
        dt = datetime.utcfromtimestamp(timestamp)


        # add it to our dict
        ranges_dict.add_date_and_id(dt, int(id, 36))



        if obj.get("author", None) is None:
            #print '!! bad author', id
            #print line
            #print "\n"
            missing_data_ids.add(id)
            missing_authors += 1

        if obj.get("subreddit", None) is None:
            print '!! subreddit', id
            print line
            print "\n"
            missing_data_ids.add(id)
            missing_subs += 1

        if comment and obj.get("link_id", None) is None:
            print '!! missing article', id
            print line
            missing_data_ids.add(id)
            missing_articles += 1

            

        # update status updater
        status_updater.count += 1
        status_updater.total_added += 1

        status_updater.update()

    
    # make sure to close the file
    infile.close()




    min_file_date = ranges_dict.years[year][month]["file_date_min"]
    max_file_date = ranges_dict.years[year][month]["file_date_max"]

    print "   [%s, %s]" %(str(min_file_date), str(max_file_date))
    if min_file_date.year != year or min_file_date.month != month:
        print "\t!! DATE oob - min", min_file_date
    if max_file_date.year != year or max_file_date.month != month:
        print "\t!! DATE oob - max", max_file_date

    if ranges_dict.years[year][month]['file_oob_dates'] > 0:
        print "\t oob dates        :", ranges_dict.years[year][month]['file_oob_dates']
    if missing_authors > 0:
        print "\t missing authors  :", missing_authors
        ranges_dict.years[year][month]['file_missing_authors'] = missing_authors
    if missing_articles > 0:
        print "\t missing articles :", missing_articles
        ranges_dict.years[year][month]['file_missing_articles'] = missing_articles
    if missing_subs > 0:
        print "\t missing subs     :", missing_subs
        ranges_dict.years[year][month]['file_missing_subs'] = missing_subs



    if args.outdir is not None:
        outdir = os.path.join(args.outdir, "%04d-%02d" % (year, month))
        if not os.path.exists(outdir):
            os.makedirs(outdir)
            f.write("\n".join(["%s,%d" %(i, int(i,36)) for i in all_ids]))


        fn = os.path.join(outdir, "all_ids.csv")
        with open(fn, "w") as f:
        fn = os.path.join(outdir, "dupcliate_ids.csv")
        with open(fn, "w") as f:
            f.write("\n".join(["%s,%d" %(i, int(i,36)) for i in dupcliate_ids]))

        fn = os.path.join(outdir, "missing_data_ids.csv")
        with open(fn, "w") as f:
            f.write("\n".join(["%s,%d" %(i, int(i,36)) for i in missing_data_ids]))


        month_summary_filename = os.path.join(outdir, "%04d-%02d.json"  % (year, month))
        with open(month_summary_filename, "w") as f:
            f.write(json.dumps(ranges_dict.years[year][month], default=str))


if args.outdir is not None:
    out_summary_filename = os.path.join(args.outdir, "summary_data.json")
    with open(out_summary_filename, "w") as f:
        f.write(json.dumps(ranges_dict.years, default=str))


#print "[%s, %s]" %(str(min_date), str(max_date))

status_updater.update(force=True)
print "Completed successfully!"

            
