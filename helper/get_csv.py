#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Source -> https://github.com/MISP/PyMISP/blob/main/examples/get_csv.py
'''

import argparse
import os

from pymisp import ExpandedPyMISP

MISP_BASE_URL = os.environ.get('MISP_BASE_URL')
MISP_API_KEY = os.environ.get('MISP_API_KEY')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Get MISP stuff as CSV.')
    parser.add_argument("--controller", default='attributes', help="Attribute to use for the search (events, objects, attributes)")
    parser.add_argument("-e", "--event_id", help="Event ID to fetch. Without it, it will fetch the whole database.")
    parser.add_argument("-a", "--attribute", nargs='+', help="Attribute column names")
    parser.add_argument("-o", "--object_attribute", nargs='+', help="Object attribute column names")
    parser.add_argument("-t", "--misp_types", nargs='+', help="MISP types to fetch (ip-src, hostname, ...)")
    parser.add_argument("-c", "--context", action='store_true', help="Add event level context (tags...)")
    parser.add_argument("-f", "--outfile", help="Output file to write the CSV.")

    args = parser.parse_args()
    pymisp = ExpandedPyMISP(str(MISP_BASE_URL), str(MISP_API_KEY), debug=True)
    attr = []
    if args.attribute:
        attr += args.attribute
    if args.object_attribute:
        attr += args.object_attribute
    if not attr:
        attr = None
    response = pymisp.search(return_format='csv', controller=args.controller, eventid=args.event_id, requested_attributes=attr,
                             type_attribute=args.misp_types, include_context=args.context)

    if args.outfile:
        with open(args.outfile, 'w') as f:
            f.write(response)
    else:
        print(response)

    print("Newest events from MISP had been loaded.")
