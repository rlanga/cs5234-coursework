#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 14 22:04:47 2020

@author: ufac001
"""

from project import extract_email_network,\
                    convert_to_weighted_network,\
                    get_out_degrees,\
                    get_in_degrees,\
                    get_out_degree_dist,\
                    get_in_degree_dist

from pyspark import SparkConf, SparkContext
from datetime import datetime, timezone
import unittest


class TestProjectCode(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Comment the following two lines out if using 
        # from Spark notebook
        conf = SparkConf().setAppName("Enron")
        cls.sc = SparkContext(conf = conf)

        pretty = lambda x: '\n'.join(str(e) for e in x) if x else None
        cls.pretty_rdd = lambda x: pretty(x.collect()) if x else None

        def utf8_decode_and_filter(g, rdd):
            def utf_decode(s):
                try:
                    return str(s, 'utf-8')
                except:
                    pass
            return rdd.map(lambda g, x: utf_decode(x[1])).filter(lambda x: x != None)
        cls.utf8_decode_and_filter = utf8_decode_and_filter

    def test_Q1(self):
        # result = self.utf8_decode_and_filter(self.sc.sequenceFile(
        #         '/user/ufac001/project1920/samples/enron1.seq'))
        result = self.pretty_rdd(extract_email_network(
                    self.utf8_decode_and_filter(self.sc.sequenceFile(
                        '/user/ufac001/project1920/samples/enron1.seq'))))

        self.assertEqual(result, 
            '''
            ('george.mcclellan@enron.com', 'mike.mcconnell@enron.com',
            datetime.datetime(2000, 7, 31, 5, 48, 
            tzinfo=datetime.timezone(datetime.timedelta(-1, 61200))))
            ('george.mcclellan@enron.com', 'jeffrey.shankman@enron.com',
            datetime.datetime(2000, 7, 31, 5, 48, 
            tzinfo=datetime.timezone(datetime.timedelta(-1, 61200))))
            ('george.mcclellan@enron.com', 'stuart.staley@enron.com', 
            datetime.datetime(2000, 7, 31, 5, 48, 
            tzinfo=datetime.timezone(datetime.timedelta(-1, 61200))))
            ('george.mcclellan@enron.com', 'daniel.reck@enron.com', 
            datetime.datetime(2000, 7, 31, 5, 48, 
            tzinfo=datetime.timezone(datetime.timedelta(-1, 61200))))
            ('george.mcclellan@enron.com', 'michael.beyer@enron.com', 
            datetime.datetime(2000, 7, 31, 5, 48, 
            tzinfo=datetime.timezone(datetime.timedelta(-1, 61200))))
            ('george.mcclellan@enron.com', 'kevin.mcgowan@enron.com', 
            datetime.datetime(2000, 7, 31, 5, 48, 
            tzinfo=datetime.timezone(datetime.timedelta(-1, 61200))))
            '''
        )

    def test_Q2_enron20(self):
        result = self.pretty_rdd(convert_to_weighted_network(
                extract_email_network(
                self.utf8_decode_and_filter(self.sc.sequenceFile(
                '/user/ufac001/project1920/samples/enron20.seq'))), 
                (datetime(2000, 10, 1, tzinfo = timezone.utc), 
                datetime(2001, 9, 1, tzinfo = timezone.utc))))

        self.assertEqual(result,
        '''
        ('george.mcclellan@enron.com', 'sven.becker@enron.com', 1)
        ('george.mcclellan@enron.com', 'stuart.staley@enron.com', 1)
        ('george.mcclellan@enron.com', 'manfred.ungethum@enron.com', 1)
        ('george.mcclellan@enron.com', 'mike.mcconnell@enron.com', 2)
        ('george.mcclellan@enron.com', 'jeffrey.shankman@enron.com', 2)
        ('stuart.staley@enron.com', 'mike.mcconnell@enron.com', 2)
        ('stuart.staley@enron.com', 'jeffrey.shankman@enron.com', 2)
        ('stuart.staley@enron.com', 'george.mcclellan@enron.com', 1)
        '''
        )

    def test_Q2_enron1(self):
        result = self.pretty_rdd(convert_to_weighted_network(
                    extract_email_network(
                        self.utf8_decode_and_filter(self.sc.sequenceFile(
                            '/user/ufac001/project1920/samples/enron1.seq')))))

        self.assertEqual(result,
        '''
        ('george.mcclellan@enron.com', 'mike.mcconnell@enron.com', 1)
        ('george.mcclellan@enron.com', 'jeffrey.shankman@enron.com', 1)
        ('george.mcclellan@enron.com', 'stuart.staley@enron.com', 1)
        ('george.mcclellan@enron.com', 'daniel.reck@enron.com', 1)
        ('george.mcclellan@enron.com', 'michael.beyer@enron.com', 1)
        ('george.mcclellan@enron.com', 'kevin.mcgowan@enron.com', 1)
        '''
        )

    def test_Q3_1(self):
        result = self.pretty_rdd(get_out_degrees(convert_to_weighted_network(
                extract_email_network(
                self.utf8_decode_and_filter(self.sc.sequenceFile(
                '/user/ufac001/project1920/samples/enron20.seq'))))))

        self.assertEqual(result,
        '''
        (26, 'george.mcclellan@enron.com')
        (8, 'mark.rodriguez@enron.com')
        (5, 'stuart.staley@enron.com')
        (2, 'john.nowlan@enron.com')
        (2, 'd.hall@enron.com')
        (2, 'cathy.phillips@enron.com')
        (1, 'mary.joyce@enron.com')
        (1, 'john.haggerty@enron.com')
        (1, 'jay.hatfield@enron.com')
        (1, 'enron.announcements@enron.com')
        (1, 'bill.cordes@enron.com')
        (0, 'tom.kearney@enron.com')
        (0, 'sven.becker@enron.com')
        (0, 'paula.harris@enron.com')
        (0, 'mike.mcconnell@enron.com')
        (0, 'michael.beyer@enron.com')
        (0, 'matthew.arnold@enron.com')
        (0, 'manfred.ungethum@enron.com')
        (0, 'kevin.mcgowan@enron.com')
        (0, 'jordan.mintz@enron.com')
        (0, 'jeffrey.shankman@enron.com')
        (0, 'deb.gebhardt@enron.com')
        (0, 'daniel.reck@enron.com')
        (0, 'angie.collins@enron.com')
        (0, 'all.houston@enron.com')
        '''
        )

    def test_Q3_2(self):
        result = self.pretty_rdd(get_in_degrees(convert_to_weighted_network(
                extract_email_network(
                self.utf8_decode_and_filter(self.sc.sequenceFile(
                '/user/ufac001/project1920/samples/enron20.seq'))))))

        self.assertEqual(result,
        '''
        (15, 'mike.mcconnell@enron.com')
        (9, 'jeffrey.shankman@enron.com')
        (4, 'stuart.staley@enron.com')
        (4, 'daniel.reck@enron.com')
        (3, 'michael.beyer@enron.com')
        (3, 'kevin.mcgowan@enron.com')
        (2, 'george.mcclellan@enron.com')
        (1, 'tom.kearney@enron.com')
        (1, 'sven.becker@enron.com')
        (1, 'paula.harris@enron.com')
        (1, 'matthew.arnold@enron.com')
        (1, 'manfred.ungethum@enron.com')
        (1, 'jordan.mintz@enron.com')
        (1, 'deb.gebhardt@enron.com')
        (1, 'cathy.phillips@enron.com')
        (1, 'angie.collins@enron.com')
        (1, 'all.houston@enron.com')
        (0, 'mary.joyce@enron.com')
        (0, 'mark.rodriguez@enron.com')
        (0, 'john.nowlan@enron.com')
        (0, 'john.haggerty@enron.com')
        (0, 'jay.hatfield@enron.com')
        (0, 'enron.announcements@enron.com')
        (0, 'd.hall@enron.com')
        (0, 'bill.cordes@enron.com')
        '''
        )

    def test_Q4_1(self):
        result = self.pretty_rdd(get_out_degree_dist(convert_to_weighted_network(
                extract_email_network(
                self.utf8_decode_and_filter(self.sc.sequenceFile(
                '/user/ufac001/project1920/samples/enron20.seq'))))))

        self.assertEqual(result,
        '''
        (0, 14)
        (1, 5)
        (2, 3)
        (5, 1)
        (8, 1)
        (26, 1)
        '''
        )

    def test_Q4_2(self):
        result = self.pretty_rdd(get_in_degree_dist(convert_to_weighted_network(
                extract_email_network(
                self.utf8_decode_and_filter(self.sc.sequenceFile(
                '/user/ufac001/project1920/samples/enron20.seq'))))))

        self.assertEqual(result,
        '''
        (0, 8)
        (1, 10)
        (2, 1)
        (3, 2)
        (4, 2)
        (9, 1)
        (15, 1)
        '''
        )

if __name__ == '__main__':
    unittest.main()
      
    
# Q1 test
# Expected output:
# ('george.mcclellan@enron.com', 'mike.mcconnell@enron.com', 
# datetime.datetime(2000, 7, 31, 5, 48, 
# tzinfo=datetime.timezone(datetime.timedelta(-1, 61200))))
# ('george.mcclellan@enron.com', 'jeffrey.shankman@enron.com',
# datetime.datetime(2000, 7, 31, 5, 48, 
# tzinfo=datetime.timezone(datetime.timedelta(-1, 61200))))
# ('george.mcclellan@enron.com', 'stuart.staley@enron.com', 
# datetime.datetime(2000, 7, 31, 5, 48, 
# tzinfo=datetime.timezone(datetime.timedelta(-1, 61200))))
# ('george.mcclellan@enron.com', 'daniel.reck@enron.com', 
# datetime.datetime(2000, 7, 31, 5, 48, 
# tzinfo=datetime.timezone(datetime.timedelta(-1, 61200))))
# ('george.mcclellan@enron.com', 'michael.beyer@enron.com', 
# datetime.datetime(2000, 7, 31, 5, 48, 
# tzinfo=datetime.timezone(datetime.timedelta(-1, 61200))))
# ('george.mcclellan@enron.com', 'kevin.mcgowan@enron.com', 
# datetime.datetime(2000, 7, 31, 5, 48, 
# tzinfo=datetime.timezone(datetime.timedelta(-1, 61200))))
    
    # Q2 test
# Expected output:
# ('george.mcclellan@enron.com', 'sven.becker@enron.com', 1)
# ('george.mcclellan@enron.com', 'stuart.staley@enron.com', 1)
# ('george.mcclellan@enron.com', 'manfred.ungethum@enron.com', 1)
# ('george.mcclellan@enron.com', 'mike.mcconnell@enron.com', 2)
# ('george.mcclellan@enron.com', 'jeffrey.shankman@enron.com', 2)
# ('stuart.staley@enron.com', 'mike.mcconnell@enron.com', 2)
# ('stuart.staley@enron.com', 'jeffrey.shankman@enron.com', 2)
# ('stuart.staley@enron.com', 'george.mcclellan@enron.com', 1)

    # Q2 test
# Expected output:
# ('george.mcclellan@enron.com', 'mike.mcconnell@enron.com', 1)
# ('george.mcclellan@enron.com', 'jeffrey.shankman@enron.com', 1)
# ('george.mcclellan@enron.com', 'stuart.staley@enron.com', 1)
# ('george.mcclellan@enron.com', 'daniel.reck@enron.com', 1)
# ('george.mcclellan@enron.com', 'michael.beyer@enron.com', 1)
# ('george.mcclellan@enron.com', 'kevin.mcgowan@enron.com', 1)
    
    
    
    # Q3.1
# Expected output:
# (26, 'george.mcclellan@enron.com')
# (8, 'mark.rodriguez@enron.com')
# (5, 'stuart.staley@enron.com')
# (2, 'john.nowlan@enron.com')
# (2, 'd.hall@enron.com')
# (2, 'cathy.phillips@enron.com')
# (1, 'mary.joyce@enron.com')
# (1, 'john.haggerty@enron.com')
# (1, 'jay.hatfield@enron.com')
# (1, 'enron.announcements@enron.com')
# (1, 'bill.cordes@enron.com')
# (0, 'tom.kearney@enron.com')
# (0, 'sven.becker@enron.com')
# (0, 'paula.harris@enron.com')
# (0, 'mike.mcconnell@enron.com')
# (0, 'michael.beyer@enron.com')
# (0, 'matthew.arnold@enron.com')
# (0, 'manfred.ungethum@enron.com')
# (0, 'kevin.mcgowan@enron.com')
# (0, 'jordan.mintz@enron.com')
# (0, 'jeffrey.shankman@enron.com')
# (0, 'deb.gebhardt@enron.com')
# (0, 'daniel.reck@enron.com')
# (0, 'angie.collins@enron.com')
# (0, 'all.houston@enron.com')

    # Q3.2
# Expected output
# (15, 'mike.mcconnell@enron.com')
# (9, 'jeffrey.shankman@enron.com')
# (4, 'stuart.staley@enron.com')
# (4, 'daniel.reck@enron.com')
# (3, 'michael.beyer@enron.com')
# (3, 'kevin.mcgowan@enron.com')
# (2, 'george.mcclellan@enron.com')
# (1, 'tom.kearney@enron.com')
# (1, 'sven.becker@enron.com')
# (1, 'paula.harris@enron.com')
# (1, 'matthew.arnold@enron.com')
# (1, 'manfred.ungethum@enron.com')
# (1, 'jordan.mintz@enron.com')
# (1, 'deb.gebhardt@enron.com')
# (1, 'cathy.phillips@enron.com')
# (1, 'angie.collins@enron.com')
# (1, 'all.houston@enron.com')
# (0, 'mary.joyce@enron.com')
# (0, 'mark.rodriguez@enron.com')
# (0, 'john.nowlan@enron.com')
# (0, 'john.haggerty@enron.com')
# (0, 'jay.hatfield@enron.com')
# (0, 'enron.announcements@enron.com')
# (0, 'd.hall@enron.com')
# (0, 'bill.cordes@enron.com')
    
    # Q4.1
# Expected output
# (0, 14)
# (1, 5)
# (2, 3)
# (5, 1)
# (8, 1)
# (26, 1)    
    
    # Q4.2
# Expected output
# (0, 8)
# (1, 10)
# (2, 1)
# (3, 2)
# (4, 2)
# (9, 1)
# (15, 1)
