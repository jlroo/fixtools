#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr  5 10:17:23 2017

@author: jlroo
"""

import multiprocessing as __mp__
from collections import defaultdict
import sys


def __build__( security_id ):
    sec_desc = __securities__[security_id]
    product = ["opt" if len(sec_desc) > 7 else "fut"][0]
    books = []
    book_obj = OrderBook(__contracts__[security_id] , security_id , product)
    for book in book_obj.build_book():
        books.append(book)
    return {security_id: books}


def __write__( security_id ):
    sec_desc = __securities__[security_id]
    product = ["opt" if len(sec_desc) > 7 else "fut"][0]
    book_obj = OrderBook(__contracts__[security_id] , security_id , product)
    filename = __securities__[security_id].replace(" " , "-")
    with open(__path__ + filename , 'ab+') as book_out:
        for book in book_obj.build_book():
            book_out.write(book)


def filter_tuple( line ):
    valid_contract = [sec if sec in line else None for sec in __securityDesc__]
    set_ids = filter(None , valid_contract)
    security_ids = set(int(sec.split(b'\x0148=')[1].split(b'\x01')[0]) for sec in set_ids)
    if b'35=X\x01' in line and any(valid_contract):
        return security_ids , line


def __filter__( line ):
    valid_contract = [sec if sec in line else None for sec in __securityDesc__]
    set_ids = iter(filter(None , valid_contract))
    security_ids = [int(sec.split(b'\x0148=')[1].split(b'\x01')[0]) for sec in set_ids]
    if any(valid_contract):
        pairs = {secid: line for secid in security_ids}
        return pairs


def _set_desc( security_desc ):
    global __securityDesc__
    __securityDesc__ = security_desc


def data_filter( data=None , contract_ids=None , processes=None , chunksize=None ):
    security_desc = [b'\x0148=' + str(sec_id).encode() + b'\x01' for sec_id in contract_ids]
    if sys.version_info[0] > 2.7:
        msgs = defaultdict(list)
        with __mp__.Pool(initializer=_set_desc , initargs=(security_desc ,)) as pool:
            filtered = pool.map(__filter__ , data , chunksize)
            for item in iter(filter(None , filtered)):
                for key in item.keys():
                    msgs[key].append(item[key])
    else:
        msgs = __mp__.Manager().dict()
        pool = __mp__.Pool(processes=processes , initializer=_set_desc , initargs=(security_desc ,))
        filtered = pool.map(__filter__ , data , chunksize)
        for item in iter(filter(None , filtered)):
            for key in item.keys():
                if key not in msgs.keys():
                    msgs[key] = []
                    msgs[key].append(item[key])
                else:
                    msgs[key].append(item[key])
        pool.close()
    try:
        data.close()
    except AttributeError:
        pass
    return msgs


def _set_writes( securities , contracts , path ):
    global __path__
    __path__ = path
    global __contracts__
    __contracts__ = contracts
    global __securities__
    __securities__ = securities


def data_book(data=None, securities=None, path=None, processes=None, chunksize_filter=None, chunksize_book=None):
    contract_ids = securities.keys()
    contracts = data_filter(data=data, contract_ids=contract_ids, processes=processes, chunksize=chunksize_filter)
    if path:
        if sys.version_info[0] > 2.7:
            with __mp__.Pool(initializer=_set_writes , initargs=(securities , contracts , path)) as pool:
                if chunksize_book is None:
                    pool.map(__write__ , contract_ids)
                else:
                    pool.map(__write__ , contract_ids , chunksize_book)
        else:
            pool = __mp__.Pool(processes=processes , initializer=_set_writes , initargs=(securities , contracts , path))
            if chunksize_book is None:
                pool.map(__write__ , contract_ids)
            else:
                pool.map(__write__ , contract_ids , chunksize_book)
            pool.close()
    else:
        if sys.version_info[0] > 2.7:
            with __mp__.Pool() as pool:
                if chunksize_book is None:
                    books = pool.map(__build__ , contract_ids)
                else:
                    books = pool.map(__build__ , contract_ids , chunksize_book)
        else:
            pool = __mp__.Pool(processes=processes)
            if chunksize_book is None:
                books = pool.map(__build__ , contract_ids)
            else:
                books = pool.map(__build__ , contract_ids , chunksize_book)
            pool.close()
        return books


class OrderBook:
    book = b''
    top_order = 0
    sec_desc_id = b''

    def __init__( self , data , security_id , product ):
        self.data = data
        self.security_id = security_id
        self.product = product.lower()

    def initial_book( self ):
        self.sec_desc_id = b'\x0148=' + str(self.security_id).encode() + b'\x01'
        msg_type = lambda e: e is not None and b'35=X\x01' in e and self.sec_desc_id in e
        trade_type = lambda e: e is not None and e[e.find(b'\x01269=') + 5:e.find(b'\x01269=') + 6] in b'0|1'
        open_msg = lambda e: msg_type(e) and trade_type(e)
        temp = b'\x01279=NA\x0122=NA' + self.sec_desc_id + b'83=NA\x01107=NA\x01269=0\x01270=NA\x01271=NA\x01273=NA\x01336=NA\x01346=NA\x011023='

        if self.product in "opt|options":
            self.top_order = 3
            prev_body = [temp + str(i).encode() for i in range(1 , 4)]
            temp = temp.replace(b'\x01269=0' , b'\x01269=1')
            prev_body = prev_body + [temp + str(i).encode() for i in range(1 , 4)]

        if self.product in "fut|futures":
            self.top_order = 10
            prev_body = [temp + str(i).encode() for i in range(1 , 11)]
            temp = temp.replace(b'\x01269=0' , b'\x01269=1')
            prev_body = prev_body + [temp + str(i).encode() for i in range(1 , 11)]

        msg = next(iter(filter(open_msg , self.data)) , None)

        if msg is not None:
            book_header = msg.split(b'\x01279')[0]
            book_end = b'\x0110' + msg.split(b'\x0110')[-1]
            msg_body = msg.split(b'\x0110=')[0].split(b'\x01279')[1:]
            msg_body = [b'\x01279' + e for e in msg_body if self.sec_desc_id in e and b'\x01276' not in e]
            msg_body = iter(filter(lambda e: trade_type(e) , msg_body))
            # BOOK UPDATE
            bids , offers = self.__update__(prev_body , msg_body)
            book_body = bids + offers
            book_header += b''.join([e for e in book_body])
            self.book = book_header + book_end
        try:
            self.data.seek(0)
        except AttributeError:
            pass
        return self.book

    def build_book( self ):
        self.book = self.initial_book()
        if self.book != b'':
            msg_seq_num = lambda line: int(line.split(b'\x0134=')[1].split(b'\x01')[0])
            book_seq_num = int(self.book.split(b'\x0134=')[1].split(b'\x01')[0])
            updates = lambda entry: entry is not None and msg_seq_num(entry) > book_seq_num
            trade_type = lambda e: e[e.find(b'\x01269=') + 5:e.find(b'\x01269=') + 6] in b'0|1'
            messages = iter(filter(updates , self.data))
            for msg in messages:
                # PREVIOUS BOOK
                prev_body = self.book.split(b'\x0110=')[0]
                prev_body = prev_body.split(b'\x01279')[1:]
                prev_body = [b'\x01279' + entry for entry in prev_body]
                book_header = msg.split(b'\x01279')[0]
                book_end = b'\x0110' + msg.split(b'\x0110')[-1]
                msg_body = msg.split(b'\x0110=')[0].split(b'\x01279')[1:]
                msg_body = [b'\x01279' + e for e in msg_body if self.sec_desc_id in e and b'\x01276' not in e]
                msg_body = iter(filter(lambda e: trade_type(e) , msg_body))
                # BOOK UPDATE
                bids , offers = self.__update__(prev_body , msg_body)
                book_body = bids + offers
                if book_body == prev_body:
                    pass
                else:
                    book_header += b''.join([e for e in book_body])
                    self.book = book_header + book_end
                    yield self.book
            try:
                self.data.seek(0)
            except AttributeError:
                pass

    def __update__( self , book_body , msg_body ):
        bids , offers = book_body[:len(book_body) // 2] , book_body[len(book_body) // 2:]
        for entry in msg_body:
            try:
                price_level = int(entry.split(b'\x011023=')[1])
                entry_type = int(entry[entry.find(b'\x01269=') + 5:entry.find(b'\x01269=') + 6])
                action_type = int(entry[entry.find(b'\x01279=') + 5:entry.find(b'\x01279=') + 6])
                temp = b'\x01279=NA\x0122=NA' + self.sec_desc_id + b'83=NA\x01107=NA\x01269=0\x01270=NA\x01271=NA\x01273=NA\x01336=NA\x01346=NA\x011023='
                if entry_type == 0:  # BID tag 269= esh9[1]
                    if action_type == 1:  # CHANGE 279=1
                        bids[price_level - 1] = entry
                    elif action_type == 0:  # NEW tag 279=0
                        if price_level == self.top_order:
                            bids[self.top_order - 1] = entry
                        else:
                            bids.insert(price_level - 1 , entry)
                            for i in range(price_level , self.top_order):
                                bids[i] = bids[i].replace(b'\x011023=' + str(i).encode() ,
                                                          b'\x011023=' + str(i + 1).encode())
                            bids.pop()
                    else:  # b'\x01279=2' DELETE
                        delete = temp + str(self.top_order).encode()
                        if price_level == self.top_order:
                            bids[self.top_order - 1] = delete
                        else:
                            bids.pop(price_level - 1)
                            for i in range(price_level , self.top_order):
                                bids[i - 1] = bids[i - 1].replace(b'\x011023=' + str(i + 1).encode() ,
                                                                  b'\x011023=' + str(i).encode())
                            bids.append(delete)
                else:  # OFFER tag 269=1
                    if action_type == 1:  # CHANGE 279=1
                        offers[price_level - 1] = entry
                    elif action_type == 0:  # NEW tag 279=0
                        if price_level == self.top_order:
                            offers[self.top_order - 1] = entry
                        else:
                            offers.insert(price_level - 1 , entry)
                            for i in range(price_level , self.top_order):
                                offers[i] = offers[i].replace(b'\x011023=' + str(i).encode() ,
                                                              b'\x011023=' + str(i + 1).encode())
                            offers.pop()
                    else:  # b'\x01279=2' DELETE
                        temp = temp.replace(b'\x01269=0' , b'\x01269=1')
                        delete = temp + str(self.top_order).encode()
                        if price_level == self.top_order:
                            offers[self.top_order - 1] = delete
                        else:
                            offers.pop(price_level - 1)
                            for i in range(price_level , self.top_order):
                                offers[i - 1] = offers[i - 1].replace(b'\x011023=' + str(i + 1).encode() ,
                                                                      b'\x011023=' + str(i).encode())
                            offers.append(delete)
            except StopIteration:
                continue
        return bids , offers
