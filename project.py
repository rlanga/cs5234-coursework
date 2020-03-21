#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 14 22:00:09 2020

@author: ufac001
"""

from email.parser import Parser
import re
import time
from datetime import datetime, timezone, timedelta

def date_to_dt(date):
    def to_dt(tms):
        def tz():
            return timezone(timedelta(seconds=tms.tm_gmtoff))
        return datetime(tms.tm_year, tms.tm_mon, tms.tm_mday, 
                      tms.tm_hour, tms.tm_min, tms.tm_sec, 
                      tzinfo=tz())
    return to_dt(time.strptime(date[:-6], '%a, %d %b %Y %H:%M:%S %z'))

# Q1: replace pass with your code
def extract_email_network(rdd):
    # Helper functions to parse emails into Tuples
    email_regex = '([!#$%&\'*+-/=?^-`{|}~.\w]+)@enron\.com'
    valid_email = lambda s: True if re.compile(email_regex).fullmatch(s) else False
    concat_csv_strings = lambda s1, s2: '{},{}'.format(s1, s2)
    val_by_vec = lambda sender, recipients, time: [(sender,r, time) for r in recipients]
    not_self_loop = lambda t: True if t[0] != t[1] else False

    def strip_non_whitespace(lst):
        if lst:
            return [email.strip() for email in lst.split(',') if email != 'None']

    rdd_mail = rdd.map(lambda s: Parser().parsestr(s))

    rdd_full_email_tuples = rdd_mail\
    .map(lambda s: (s.get('From'), concat_csv_strings(s.get('To'),
        concat_csv_strings(s.get('Cc'), s.get('Bcc'))), date_to_dt(s.get('Date'))))\
    .map(lambda header: (header[0], strip_non_whitespace(header[1]), header[2]))

    rdd_email_triples = rdd_full_email_tuples.flatMap(lambda header: val_by_vec(header[0], header[1], header[2]))

    both_valid_emails = lambda header: True if valid_email(header[0]) and valid_email(header[1]) else False
    rdd_email_triples_enron = rdd_email_triples.filter(lambda header: not_self_loop(header))\
        .filter(lambda header: both_valid_emails(header))
    
    return rdd_email_triples_enron.distinct()
        

# Q2: replace pass with your code
def convert_to_weighted_network(rdd, drange=None):
    if drange:
        rdd = rdd.filter(lambda header: drange[0] <= header[2] and header[2] <= drange[1])
    return rdd.map(lambda header: ((header[0], header[1]), 1)) \
        .reduceByKey(lambda x, y: x + y) \
        .map(lambda header: (header[0][0], header[0][1], header[1]))

# Q3.1: replace pass with your code
def get_out_degrees(rdd):
    return rdd.map(lambda node: (node[0], node[2])) \
        .reduceByKey(lambda x, y: x + y) \
        .map(lambda node: (node[1], node[0])) \
        .sortBy(lambda node: node, ascending=False)

# Q3.2: replace pass with your code         
def get_in_degrees(rdd):
    return rdd.map(lambda node: (node[1], node[2])) \
        .reduceByKey(lambda x, y: x + y) \
        .map(lambda node: (node[1], node[0])) \
        .sortBy(lambda node: node, ascending=False)

# Q4.1: replace pass with your code            
def get_out_degree_dist(rdd):
    return get_out_degrees(rdd).map(lambda count: (count[0], 1)) \
        .reduceByKey(lambda x, y: x + y) \
        .sortBy(lambda d: d[0])

# Q4.2: replace pass with your code
def get_in_degree_dist(rdd):
    return get_in_degrees(rdd).map(lambda count: (count[0], 1)) \
        .reduceByKey(lambda x, y: x + y) \
        .sortBy(lambda d: d[0])
