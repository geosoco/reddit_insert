#!/bin/bash


if [ $# -lt 2  ]
then
	echo "usage: $0 <input file> <output path>"
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
        python -u extract_oob_dates.py "$F" "$2"
        retval=${PIPESTATUS[0]}
    elif [ $ext = 'xz' ]; then
        python -u extract_oob_dates.py "$F" "$2"
        retval=${PIPESTATUS[0]}
    elif [ $ext = 'zst' ]; then
        python -u extract_oob_dates.py "$F" "$2"
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

done <$1

