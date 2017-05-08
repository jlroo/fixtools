#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 24 12:52:01 2017

@author: jlroo
"""
import multiprocessing as mp
from fixtools import __secfilter__,__update__,initialbook

class Futures:
    book = ""
    sec_desc_id = b''
    product = "futures"

    def __init__(self, data, security_id):
        self.data = data
        self.securitid_id = security_id

    def buildbook(self, chunksize = 10 ** 4):
        msg_seq_num = lambda line: int(line.split(b'\x0134=')[1].split(b'\x01')[0])
        book = initialbook(self.product,self.securitid_id,self.data)
        book_seq_num = int(book.split(b'\x0134=')[1].split(b'\x01')[0])
        updates = lambda entry: entry is not None and msg_seq_num(entry) > book_seq_num
        trade_type = lambda e: e[e.find(b'\x01269=') + 5:e.find(b'\x01269=') + 6] in b'0|1'
        with mp.Pool() as pool:
            msg_map = pool.imap(__secfilter__, self.data, chunksize)
            messages = iter(filter(updates, msg_map))
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
                bids, offers = __update__(self.product, self.security_id,prev_body, msg_body)
                book_body = bids + offers
                if book_body == prev_body:
                    pass
                else:
                    book_header += b''.join([e for e in book_body])
                    self.book = book_header + book_end
                    yield self.book
        self.data.seek(0)
