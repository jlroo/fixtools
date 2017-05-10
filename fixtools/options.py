#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 24 12:50:00 2017

@author: jlroo
"""

import multiprocessing as mp
from fixtools.book import __update__, build_book


def options_books(data, contracts, top_order):
	books = {}
	contracts = set(contracts)
	messages = iter(filter(lambda e: e is not None, data))
	for update_msg in messages:
	    msg_body = update_msg.split(b'\x0110=')[0].split(b'\x01279')[1:]
	    inner_messages = [int(i.split(b'\x0148=')[1].split(b'\x01')[0]) for i in msg_body]
	    for sec_id in inner_messages:
	        if sec_id in contracts:
	            if  sec_id not in books.keys():
	                book0 = Options(update_msg, str(sec_id)).book
	                book = build_book(book0, update_msg, str(sec_id), top_order)
	                if book is None:
	                    pass
	                else:
	                    books[sec_id] = {"book": []}
	                    books[sec_id]["book"].append(book)
	            else:
	                prev_book = books[sec_id]["book"][-1]
	                book = build_book(prev_book, update_msg, str(sec_id), top_order)
	                if book is None:
	                    pass
	                else:
	                    books[sec_id]["book"].append(book)
	return books


class Options:
	book = b''
	product = "options"
	top_order = 3

	def __init__(self, data, security_id):
		self.data = data
		self.sec_desc_id = b'\x0148=' + security_id.encode() + b'\x01'
		head = b'1128=NA\x019=NA\x0135=NA\x0149=NA\x0134=0\x0152=00000000000000000\x0175=00000000\x01268=NA'
		temp_body = b'\x01279=NA\x0122=NA' + self.sec_desc_id + \
		            b'83=NA\x01107=NA\x01269=0\x01270=NA\x01271=NA\x01273=NA\x01336=NA\x01346=NA\x011023='
		body = [temp_body + str(i).encode() for i in range(1, self.top_order + 1)]
		temp_body = temp_body.replace(b'\x01269=0', b'\x01269=1')
		body = body + [temp_body + str(i).encode() for i in range(1, self.top_order + 1)]
		body = b"".join([e for e in body])
		self.book = head + body + b'\x0110=000\n'

	def build_book(self, chunksize=10 ** 4):
		msg_seq_num = lambda line: int(line.split(b'\x0134=')[1].split(b'\x01')[0])
		# book = initialbook(self.product,self.securitid_id,self.data)
		book_seq_num = int(self.book.split(b'\x0134=')[1].split(b'\x01')[0])
		updates = lambda entry: entry is not None and msg_seq_num(entry) > book_seq_num
		trade_type = lambda e: e[e.find(b'\x01269=') + 5:e.find(b'\x01269=') + 6] in b'0|1'
		with mp.Pool() as pool:
			# msg_map = pool.imap(__secfilter__, self.data, chunksize)
			messages = iter(filter(updates, self.data))
			for msg in messages:
				# PRIVIOUS BOOK
				prev_body = self.book.split(b'\x0110=')[0]
				prev_body = prev_body.split(b'\x01279')[1:]
				prev_body = [b'\x01279' + entry for entry in prev_body]
				book_header = msg.split(b'\x01279')[0]
				book_end = b'\x0110' + msg.split(b'\x0110')[-1]
				msg_body = msg.split(b'\x0110=')[0].split(b'\x01279')[1:]
				msg_body = [b'\x01279' + e for e in msg_body if self.sec_desc_id in e and b'\x01276' not in e]
				msg_body = iter(filter(lambda e: trade_type(e), msg_body))
				# BOOK UPDATE
				bids, offers = __update__(prev_body, msg_body, self.sec_desc_id, self.top_order)
				book_body = bids + offers
				if book_body == prev_body:
					pass
				else:
					book_header += b''.join([e for e in book_body])
					self.book = book_header + book_end
					yield self.book
		self.data.seek(0)
