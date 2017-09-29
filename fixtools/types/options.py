#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 24 12:50:00 2017

@author: jlroo
"""

from fixtools.core.book import OrderBook
from fixtools.util.util import initial_book
from fixtools.util.util import filter_securities

class Options:
    book = b''
    product = "options"
    top_order = 3
    sec_desc_id = b''

    def __init__(self, data, security_id, chunksize = 10 ** 4):
        self.secid = security_id
        self.data = filter_securities(data, security_id, chunksize)

    def initial_book(self):
        self.book = initial_book(self.data, self.secid, self.product)
        return self.book

    def build_book(self):
        book_obj = OrderBook(self.data, self.secid, self.product)
        return book_obj.build_book()