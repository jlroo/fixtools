#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 24 12:52:01 2017

@author: jlroo
"""

from fixtools.core.book import OrderBook
from fixtools.util.util import initial_book
from fixtools.util.util import filter_securities


class Futures:
    book = b''
    product = "futures"
    top_order = 10
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