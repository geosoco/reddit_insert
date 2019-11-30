#!/bin/bash

#read -s -p "Enter password: " password

if [ $# -lt 2  ]
then
	echo "usage: $0 <database> <text file with filenames to import>"
	exit 1
fi

while read F; do
    filename="$(basename -- "$F")"
    ext="${filename##*.}"

    echo
    echo
    echo "========================"
    echo $filename
    echo "========================"
    echo

    retval=0
    if [ $ext = 'bz2' ]; then
        python -u reddit_part_copy.py $1 <(bunzip2 -c "$F") --pseudo "$filename" 2>&1 | tee -a "logs/$filename.log"
        retval=${PIPESTATUS[0]}
    elif [ $ext = 'xz' ]; then
        python -u reddit_part_copy.py $1 <(xz -d -c "$F") --pseudo "$filename" 2>&1 | tee -a "logs/$filename.log"
        retval=${PIPESTATUS[0]}
    elif [ $ext = 'zst' ]; then
        python -u reddit_part_copy.py $1 <(zstd -d -k -c "$F") --pseudo "$filename" 2>&1 | tee -a "logs/$filename.log"
        retval=${PIPESTATUS[0]}
    else
        echo "UNKNOWN EXTENSION: $ext"
        exit 1
    fi


    if [ $retval -ne 0 ]; then
        echo "ERROR"
        exit $retval
    fi
    
    #echo "$filename ... $ext}"

done <$2

