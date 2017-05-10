#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 24 12:50:00 2017

@author: jlroo
"""

from fixtools.book import OrderBook, build_book


class Options:
	book = b''
	product = "options"
	top_order = 3
	sec_desc_id = b''

	def __init__(self, data, security_id):
		self.security_id = security_id
		self.data = data
		self.sec_desc_id = b'\x0148=' + self.security_id.encode() + b'\x01'
		head = b'1128=NA\x019=NA\x0135=NA\x0149=NA\x0134=0\x0152=00000000000000000\x0175=00000000\x01268=NA'
		temp_body = b'\x01279=NA\x0122=NA' + self.sec_desc_id + b'83=NA\x01107=NA\x01269=0\x01270=NA\x01271=NA\x01273=NA\x01336=NA\x01346=NA\x011023='
		body = [temp_body + str(i).encode() for i in range(1, self.top_order + 1)]
		temp_body = temp_body.replace(b'\x01269=0', b'\x01269=1')
		body = body + [temp_body + str(i).encode() for i in range(1, self.top_order + 1)]
		body = b"".join([e for e in body])
		self.book = head + body + b'\x0110=000\n'

	def build_book(self):
		book_obj = OrderBook(self.data, self.security_id, self.product)
		return book_obj.build_book()

	def build_books(self, filtered_data, contracts):
		books = {}
		contracts = set(contracts)
		messages = iter(filter(lambda e: e is not None, filtered_data))
		for update_msg in messages:
			msg_body = update_msg.split(b'\x0110=')[0].split(b'\x01279')[1:]
			inner_messages = [int(i.split(b'\x0148=')[1].split(b'\x01')[0]) for i in msg_body]
			for sec_id in inner_messages:
				if sec_id in contracts:
					if sec_id not in books.keys():
						book0 = Options(update_msg, str(sec_id)).book
						book = build_book(book0, update_msg, str(sec_id), self.top_order)
						if book is None:
							pass
						else:
							books[sec_id] = {"book": []}
							books[sec_id]["book"].append(book)
					else:
						prev_book = books[sec_id]["book"][-1]
						book = build_book(prev_book, update_msg, str(sec_id), self.top_order)
						if book is None:
							pass
						else:
							books[sec_id]["book"].append(book)
		return books
