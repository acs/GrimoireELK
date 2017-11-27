#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Tool to check raw index mappings
#
# Copyright (C) 2017 Bitergia
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
# Authors:
#   Alvaro del Castillo San Felix <acs@bitergia.com>
#

import argparse
import json
import logging
import sys


import requests

from grimoire_elk.elk.elastic import ElasticSearch
from grimoire_elk.utils import get_connectors, get_connector_name_from_cls_name

DEFAULT_LIMIT = 1000

def get_params():
    parser = argparse.ArgumentParser(usage="usage:index_raw-check.py [options]",
                                     description="Check mappings in raw indexes")
    parser.add_argument("-e", "--elastic-url", required=True, help="ElasticSearch URL")
    parser.add_argument('-g', '--debug', dest='debug', action='store_true')
    args = parser.parse_args()

    return args

def check_mapping(es_url, index_name):

    def number_fields(dictionary):
        nfields = 0

        for field in dictionary:
            if isinstance(dictionary[field], dict):
                nfields += number_fields(dictionary[field])
            else:
                nfields += 1
        return nfields

    # Get mapping
    res = requests.get(es_url +"/"+index_name+"/_mapping")
    res.raise_for_status()
    print("Fields for %s: %i" % (index_name, number_fields(res.json())))
    # print(json.dumps(res.json(), indent=True))

def check_mappings(es_url):

    # Get all mappings
    res = requests.get(es_url +"/_cat/indices?v&format=json")
    res.raise_for_status()

    for index in res.json():
        if index['index'].endswith('-raw'):
            check_mapping(es_url, index['index'])

if __name__ == '__main__':

    ARGS = get_params()

    if ARGS.debug:
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(message)s')
        logging.debug("Debug mode activated")
    else:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)

    check_mappings(ARGS.elastic_url)
