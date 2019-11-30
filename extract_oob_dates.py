#!/usr/bin/env python3
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
    print('\n'.join([hex_view(a).ljust(n*2+n-1) + "  " + char_view(a) for a in grouper(n, m)]))




def write_line(outdir, filetype, file_dt, line_dt, line):
    dirname = "R{}_{}".format(
        filetype,
        line_dt.strftime("%Y-%m")
        )
    path = os.path.join(outdir, fname)
    os.makedirs(path, exist_ok=True)

    filename = "R{}_{}".format(
        filetype,
        file_dt.strftime("%Y-%m"))

    out_file_path = os.path.join(path, filename)

    with open(out_file_path, "a+") as outfile:
        outfile.write(line + "\n")




my_whitespace = string.whitespace + "\0"

#
# PARSER
#

parser = argparse.ArgumentParser(description="extract out-of-range date data")
parser.add_argument('filename', help='filename to import')
parser.add_argument('outdir', help="optional output directory for missing data files")
parser.add_argument('--pseudo', help='actual filename to import')


args = parser.parse_args()


# set up table name data
fn = args.filename
if fn == "-":
    if not args.hasattr("pseudo") or args.pseudo is none:
        print("if using stdin, you must specify a pseudo filename using --pseudo")
        quit(1)
    else:
        fn = args.pseudo



mo = re.match("R([SC])_(\d{4})-(\d{2}).*", os.path.basename(fn), re.I)
if mo is None:
    mo = re.match("R([SC])_(\d{4})-(\d{2}).*", args.pseudo, re.I)
    if mo is None:
        print("Couldn't parse date from filename. (expects R[SC]_YYYY-MM.*)", os.path.basename(fn))
        quit(1)


file_type = mo.group(1).upper()

table_dt = datetime(int(mo.group(2)), int(mo.group(3)), 1, tzinfo=timezone.utc)
next_table_dt = table_dt + relativedelta(months=1)

# set default buffer size before opening files
io.DEFAULT_BUFFER_SIZE = io_buffer_size


status_updater = StatusUpdater()
status_updater.total_files = 1


if args.filename.endswith(".bz2"):
    print("detected bz2 file...")
    #infile = bz2.BZ2File(args.filename, "rt")
    infile = bz2.open(args.filename, "rt", encoding="utf8")
    file_length = 0
elif args.filename.endswith(".xz"):
    print("detected xz file...")
    infile = lzma.open(args.filename, "r", encoding="utf8")
    file_length = 0
else:
    infile = open(args.filename, "r")

    file_length = 0
    if stat.S_ISFIFO(os.stat(args.filename).st_mode) is False:
        # get file length
        try:
            infile.seek(0, os.SEEK_END)
            file_length = infile.tell()
            infile.seek(0, os.SEEK_SET)
        except io.UnsupportedOperation:
            file_length = 0
    else:
        fix_linux_pipe(infile)


    while True:
        all_lines =  [s.strip(my_whitespace) for s in islice(infile, chunk_size)]

        if len(all_lines) == 0:
            break

        for l in all_lines:
            #line = line.strip(my_whitespace)
#           if "\\u0000" in line or "\u0000" in line or "\0" in line:
#               line = line.replace("\\u0000", "\\\\u0000")
#               line = line.replace("\u0000", "\\\\u0000")
#               line = line.replace("\0", "<<::NULL::>>")


            try:
                obj = json.loads(line)
            except ValueError as e:
                print("Couldn't read line")
                print("-"*50)
                print(line)
                print("-"*50)
                hexdump(line)

                continue

            # get timestamp
            timestamp = int(obj["created_utc"])
            dt = datetime.utcfromtimestamp(timestamp)


            if dt < table_dt or dt >= next_table_dt:
                write_line(
                    args.outdir,
                    file_type,
                    table_dt,
                    dt,
                    )
                status_updater.total_added += 1


            # update status updater
            status_updater.count += 1

            status_updater.update()

