This project is for getting reddit data into postgres. This has been mostly used with [pushshift data](https://files.pushshift.io) and is focused around Postgres.

The most up-to-date version in here is the script is the `reddit_part_copy.py` . 

There's also a couple of other scripts in here, which may need to be updated to python 3 that do a number of things including some checks on the data itself. In the original release of the file dataset, there were a few things that were off including files containing data for other months, duplicate lines, etc. 

Some of the work in here attempts to optimize the usage of this data, such as making sure the data is clustered by the id (Which can be somewhat problematic in the earlier years where ids tend to jump or work backwards over time). These make a relatively small amount, so it's generally more beneficial to have them all run via `created_utc` increasing. 


## reddit_part_copy.py

This file uses postgres' binary copy method to insert comments and submissions into year_month partitions. It creates unlogged tables for the partition, copies, clusters, adds log, then adds indexes and it can do it pretty quickly. 

While it can handle compressed files, you can get a significant performance gain on a linux system (or the Windows Linux Subsystem) by using [process substition](https://www.linuxjournal.com/content/shell-process-redirection) to have the decompression done in a separate process. This creates a named pipe/FIFO, a second process, and let's the second process decompress into the pipe. The script itself will attempt to bump the pipe size up significantly so bzip can decompress on it's own, keeping data ready for reading by the database inserter. 

Here's an example commandline:

`python reddit_part_copy.py reddit <(bunzip2 -c RC_2010-07.bz2) -u insert_user -p -s 256 --pseudo RC_2010-07.bz2` 


The binary copy version is almost 2x faster than the bulk insert based on my benchmarks using an older computer and an SSD, which includes a number of extracted columns plus the raw data into a jsonb column. 

