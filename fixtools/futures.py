#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 24 12:52:01 2017

@author: jlroo
"""

from fixtools.book import OrderBook


class Futures:
	book = b''
	product = "futures"
	top_order = 10
	sec_desc_id = b''

	def __init__(self, data, security_id):
		self.security_id = security_id
		self.data = data
		self.sec_desc_id = b'\x0148=' + self.security_id.encode() + b'\x01'

		head = b'1128=NA\x019=NA\x0135=NA\x0149=NA\x0134=0\x0152=00000000000000000\x0175=00000000\x01268=NA'
		temp_body = b'\x01279=NA\x0122=NA' + self.sec_desc_id + \
		            b'83=NA\x01107=NA\x01269=0\x01270=NA\x01271=NA\x01273=NA\x01336=NA\x01346=NA\x011023='
		body = [temp_body + str(i).encode() for i in range(1, self.top_order + 1)]
		temp_body = temp_body.replace(b'\x01269=0', b'\x01269=1')
		body = body + [temp_body + str(i).encode() for i in range(1, self.top_order + 1)]
		body = b"".join([e for e in body])
		self.book = head + body + b'\x0110=000\n'

	def build_book(self):
		book_obj = OrderBook(self.data, self.security_id, self.product)
		return book_obj.build_book()
