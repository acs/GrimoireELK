#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
#
# Copyright (C) 2015 Bitergia
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

import logging
from .enrich import Enrich, metadata

logger = logging.getLogger(__name__)


class MarvelEnrich(Enrich):

    @metadata
    def get_rich_item(self, item):
        eitem = {}

        for f in self.RAW_FIELDS_COPY:
            if f in item:
                eitem[f] = item[f]
            else:
                eitem[f] = None

        comic = item['data']

        # Fields that are the same in item and eitem
        copy_fields = ["urls", "title", "modified"]
        for f in copy_fields:
            if f in comic:
                eitem[f] = comic[f]
            else:
                eitem[f] = None
        # Fields which names are translated
        map_fields = {"title": "comic_title",
                      "id": "comic_id"}
        for fn in map_fields:
            if fn in comic:
                eitem[map_fields[fn]] = comic[fn]
            else:
                eitem[map_fields[fn]] = None

        # Thumbnails
        eitem['url_thumbnail'] = item['data']['thumbnail']['path']

        return eitem

    def get_rich_comic_creator(self, item, creator):
        ecreator = self.get_rich_item(item)  # reuse all fields from item
        ecreator['id'] = item['uuid'] + "_" + creator['name']
        ecreator['role'] = creator['role']
        ecreator['name'] = creator['name']
        ecreator['resourceURI'] = creator['resourceURI']

        return ecreator

    def get_rich_item_creators(self, item):
        creators_enrich = []

        if 'creators' not in item['data']:
            return creators_enrich

        for creator in item['data']['creators']['items']:
            ecreator = self.get_rich_comic_creator(item, creator)
            creators_enrich.append(ecreator)

        return (creators_enrich)

    def enrich_items(self, ocean_backend):
        """ For each comic create an enriched item for each of the creators"""
        items = ocean_backend.fetch()
        ncreators = 0
        rich_item_creators = []

        for item in items:
            creators = self.get_rich_item_creators(item)
            rich_item_creators += creators

        if rich_item_creators:
            ncreators = self.elastic.bulk_upload(rich_item_creators, "id")
        logger.info("Total creators enriched: %i", ncreators)
