import re
import time
import datetime
import glob
import os
import argparse
import sys
import logsparseLib

sys.path.append('/System/Library/Frameworks/Python.framework/Versions/2.7/lib/python2.7/')
find_date = re.compile('(\d{6})(\d{2})\.log.+(\d\d):(\d\d).(\d+)-(\d+)')

def parse_params():
    parser = argparse.ArgumentParser()
    parser.add_argument('-D', '--duration', type=int,
                        help='Duration of event in microsec')
    parser.add_argument('-s', '--seduction_date', type=str,
                        help='Date for seduction. 1903061806:52.219016')
    parser.add_argument('-d', '--debug', action='store_true',
                        help='debug mode', default=False)
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='verbose mode', default=False)
    parser.add_argument('globs', type=str, nargs='*')
    loc_params = parser.parse_args()
    return loc_params


def def_date(date):
    date = re.sub(r'[^\d\:\.]', '', date)
    date = datetime.datetime(2000+int(date[0:2]), int(date[2:4]), int(date[4:6]),
                            int(date[6:8]), int(date[8:10]), int(date[11:13]),
                            int(date[14:]))
    return date

if __name__ == '__main__':
    params = parse_params()
    datetime_for_seduction = def_date(params.seduction_date)
    duration_seconds = params.duration / 1000000
    duration_microseconds = params.duration % 1000000
    timedelta_for_seduction = datetime.timedelta(0, duration_seconds, duration_microseconds)

    str_events = logsparseLib.read_events_from_files(params.globs, filter_operation='')
    for str_event in str_events:
        event = logsparseLib.Event(str_event)
        if  datetime_for_seduction >= event.datetime and datetime_for_seduction - event.datetime <= timedelta_for_seduction:
            print str_event
        