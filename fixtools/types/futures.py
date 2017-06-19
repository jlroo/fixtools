#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 24 12:52:01 2017

@author: jlroo
"""

from fixtools.core.book import OrderBook
from fixtools.util.util import initial_book


class Futures:
	book = b''
	product = "futures"
	top_order = 10
	sec_desc_id = b''

	def __init__(self, data):
		self.data = data

	def initial_book(self, security_id):
		self.book = initial_book(self.data, security_id, self.product)
		return self.book

	def build_book(self, security_id):
		book_obj = OrderBook(self.data, security_id, self.product)
		return book_obj.build_book()
