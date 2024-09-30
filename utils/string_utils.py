#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

"""
===============================================================================
# String Helpers
#
# A set of wrappers for strings
#
#===============================================================================
"""


def fix_unicode_string(line):
    """
    Fixes null escapes appearing from json that gets interpreted by postgres
    """
    s = re.sub(r"(?<!\\)\\u0000", " ", line)
    s = re.sub("\00", "<<:NULL:>>", s)
    s = s.replace("\0", "<<:NULL:>>")

    return s.replace('"', '""')


def fix_null_in_unicode(line):
    """
    Fixes null escapes appearing from json that gets interpreted by postgres
    """
    s = re.sub(r"(?<!\\)\\u0000", " ", line.strip("").strip("\0"))
    s = re.sub("\0", "<<:NULL:>>", s)
    return s

    #return line.replace("\\u0000", "\\\\u0000").replace("\0", "<<:NULL:>>").replace("\x00", "<<:NULL:>>")
