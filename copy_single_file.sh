#!/bin/bash

if [ $# -lt 2  ]
then
	echo "usage: $0 <database> <text file with filenames to import>"
	exit 1
fi


filename="$(basename -- "$2")"
ext="${filename##*.}"

echo
echo
echo "========================"
echo $filename
echo "========================"
echo

retval=0
if [ $ext = 'bz2' ]; then
    python -u reddit_part_copy.py $1 <(bunzip2 -c "$2") --pseudo "$filename" 2>&1 | tee -a "logs/$filename.log"
    retval=${PIPESTATUS[0]}
elif [ $ext = 'xz' ]; then
    python -u reddit_part_copy.py $1 <(xz -d -c "$2") --pseudo "$filename" 2>&1 | tee -a "logs/$filename.log"
    retval=${PIPESTATUS[0]}
elif [ $ext = 'zst' ]; then
    python -u reddit_part_copy.py $1 <(zstd -d -k -c "$2") --pseudo "$filename" 2>&1 | tee -a "logs/$filename.log"
    retval=${PIPESTATUS[0]}
else
    echo "UNKNOWN EXTENSION: $ext"
    exit 1
fi