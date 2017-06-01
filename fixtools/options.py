#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 24 12:50:00 2017

@author: jlroo
"""

import multiprocessing as mp
from fixtools.book import OrderBook

option_contracts = None

SecurityID = None


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
		global SecurityID
		SecurityID = security_id
		self.sec_desc_id = b'\x0148=' + SecurityID.encode() + b'\x01'
		head = b'1128=NA\x019=NA\x0135=NA\x0149=NA\x0134=0\x0152=00000000000000000\x0175=00000000\x01268=NA'
		temp_body = b'\x01279=NA\x0122=NA' + self.sec_desc_id + b'83=NA\x01107=NA\x01269=0\x01270=NA\x01271=NA\x01273=NA\x01336=NA\x01346=NA\x011023='
		body = [temp_body + str(i).encode() for i in range(1, self.top_order + 1)]
		temp_body = temp_body.replace(b'\x01269=0', b'\x01269=1')
		body = body + [temp_body + str(i).encode() for i in range(1, self.top_order + 1)]
		body = b"".join([e for e in body])
		self.book = head + body + b'\x0110=000\n'
		return self.book

	def build_book(self, security_id):
		global SecurityID
		SecurityID = security_id
		book_obj = OrderBook(self.data, security_id, self.product)
		return book_obj.build_book()

	def build_books(self, contracts, chunksize=10 ** 4):
		books = {}
		global option_contracts
		option_contracts = contracts
		with mp.Pool() as pool:
			filtered = pool.map(__options_filter__, self.data, chunksize)
		for sec_id in contracts:
			global SecurityID
			SecurityID = sec_id
			books[sec_id] = []
			self.data = filter(None, filtered)
			book_generator = self.build_book(sec_id)
			for book in book_generator:
				books[sec_id].append(book)
		if type(self.data) != filter:
			self.data.seek(0)
		return books
