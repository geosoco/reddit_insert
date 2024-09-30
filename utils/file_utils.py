#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-
"""
===============================================================================
# FileTools
#
# A collection of file utilities
#
#===============================================================================
"""

import stat
import sys
import os
import fcntl
import platform


__DEFAULT_PIPE_SIZE = 1024*1024


def is_pipe(filename):
    """
    returns true if file is a pipe
    """
    return stat.S_ISFIFO(os.stat(filename).st_mode)


def set_pipe_size(fd, size=__DEFAULT_PIPE_SIZE):
    """
    This function attempts to increase the pipe buffer size
    """
    if platform.system() == 'Linux':

        if not hasattr(fcntl, 'F_SETPIPE_SZ'):
            fcntl.F_SETPIPE_SZ = 1031

        if not hasattr(fcntl, 'F_GETPIPE_SZ'):
            fcntl.F_GETPIPE_SZ = 1032


        old_size = fcntl.fcntl(fd, fcntl.F_GETPIPE_SZ)
        fcntl.fcntl(fd, fcntl.F_SETPIPE_SZ, size)
        new_size = fcntl.fcntl(fd, fcntl.F_GETPIPE_SZ)

        print("changed pipesize from {} to {}".format(old_size, new_size))





