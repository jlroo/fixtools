#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 24 12:50:00 2017

@author: jlroo
"""

import multiprocessing as mp
from fixtools.book import OrderBook
from fixtools.util import initial_book

option_contracts = None


def __options_filter__(line):
	global option_contracts
	sec_desc = [b'\x0148=' + sec_id.encode() + b'\x01' for sec_id in option_contracts]
	valid_option = [sec in line for sec in sec_desc]
	mk_refresh = b'35=X\x01' in line
	if mk_refresh and any(valid_option):
		return line


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

	def build_books(self, contracts, chunksize=10 ** 4):
		books = {}
		global option_contracts
		option_contracts = contracts
		with mp.Pool() as pool:
			filtered = pool.map(__options_filter__, self.data, chunksize)
		for sec_id in contracts:
			books[sec_id] = []
			self.data = filter(None, filtered)
			book_generator = self.build_book(sec_id)
			for book in book_generator:
				books[sec_id].append(book)
		try:
			self.data.seek(0)
		except AttributeError:
			pass
		return books
