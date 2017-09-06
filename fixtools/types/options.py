#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 24 12:50:00 2017

@author: jlroo
"""

from fixtools.core.book import OrderBook
from fixtools.util.util import initial_book


class Options:
    book = b''
    product = "options"
    top_order = 3
    sec_desc_id = b''

    def __init__(self, data):
        self.data = data

    def initial_book(self, security_id):
        self.book = initial_book(self.data, security_id, self.product)
        return self.book

    def build_book(self, security_id):
        book_obj = OrderBook(self.data, security_id, self.product)
        return book_obj.build_book()
