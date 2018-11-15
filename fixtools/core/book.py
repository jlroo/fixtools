#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr  5 10:17:23 2017

@author: jlroo
"""

import sys
import numpy as __np__
from pandas import Timestamp
import datetime as __datetime__
import multiprocessing as __mp__
from collections import defaultdict
from fixtools.util.util import expiration_date


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


def __orderdict__( item , codes ):
    sec_desc = str(item['security_desc'])
    strike_price = int(sec_desc.split(" ")[1][1:])
    order_type = sec_desc.split(" ")[1][0]
    trade_day = str(item['trade_date'])
    year = int(trade_day[0:4])
    month = int(trade_day[4:6])
    day = int(trade_day[6:])
    trade_date = __datetime__.datetime(year , month , day)
    month_exp = codes[sec_desc[2].lower()]
    if month == 12:
        year += 1
    exp_date = expiration_date(year , month_exp , 3 , day='friday')
    delta = exp_date - trade_date
    if "c" in order_type.lower():
        dd = {"strike_price": strike_price ,
              "trade_date": trade_date ,
              "exp_date": exp_date ,
              "exp_days": delta.days ,
              "opt_c_sec_id": item['security_id'] ,
              "opt_c_desc": sec_desc ,
              "opt_c_msg_seq_num": item['msg_seq_num'] ,
              "opt_c_sending_time": str(item['sending_time']) ,
              "opt_c_bid_price": item['bid_price'] ,
              "opt_c_bid_size": item['bid_size'] ,
              "opt_c_bid_level": item['bid_level'] ,
              "opt_c_offer_price": item['offer_price'] ,
              "opt_c_offer_size": item['offer_size'] ,
              "opt_c_offer_level": item['offer_level']}
    else:
        dd = {"strike_price": strike_price ,
              "trade_date": trade_date ,
              "exp_date": exp_date ,
              "exp_days": delta.days ,
              "opt_p_sec_id": item['security_id'] ,
              "opt_p_desc": sec_desc ,
              "opt_p_msg_seq_num": item['msg_seq_num'] ,
              "opt_p_sending_time": str(item['sending_time']) ,
              "opt_p_bid_price": item['bid_price'] ,
              "opt_p_bid_size": item['bid_size'] ,
              "opt_p_bid_level": item['bid_level'] ,
              "opt_p_offer_price": item['offer_price'] ,
              "opt_p_offer_size": item['offer_size'] ,
              "opt_p_offer_level": item['offer_level']}
    return dd


def __bookdict__( item=None , codes=None ):
    """
    Creates a dictionary from the FIX order book
    :param item:
    :param codes:
    :return: Return python dictionary with the time to expiration
    """
    if codes is None:
        month_codes = "F,G,H,J,K,M,N,Q,U,V,X,Z".lower()
        codes = {k[1]: k[0] for k in enumerate(month_codes.rsplit(",") , 1)}
    sec_desc = str(item['security_desc'])
    trade_day = str(item['trade_date'])
    year , month , day = int(trade_day[0:4]) , int(trade_day[4:6]) , int(trade_day[6:])
    trade_date = __datetime__.datetime(year , month , day)
    month_exp = codes[sec_desc[2].lower()]
    if month == 12:
        year = year + 1
    exp_date = expiration_date(year , month_exp , 3 , day='friday')
    delta = exp_date - trade_date
    dd = {"trade_date": trade_date ,
          "exp_days": delta.days ,
          "fut_sec_id": item['security_id'] ,
          "fut_sec_desc": item['security_desc'] ,
          "fut_msg_seq_num": item['msg_seq_num'] ,
          "fut_sending_time": str(item['sending_time']) ,
          "fut_bid_price": item['bid_price'] ,
          "fut_bid_size": item['bid_size'] ,
          "fut_bid_level": item['bid_level'] ,
          "fut_offer_price": item['offer_price'] ,
          "fut_offer_size": item['offer_size'] ,
          "fut_offer_level": item['offer_level']}
    columns = ["opt_p_sec_id" , "opt_p_desc" ,
               "opt_p_msg_seq_num" , "opt_p_sending_time" ,
               "opt_p_bid_price" , "opt_p_bid_size" ,
               "opt_p_bid_level" , "opt_p_offer_price" ,
               "opt_p_offer_size" , "opt_p_offer_level" ,
               "opt_c_sec_id" , "opt_c_desc" ,
               "opt_c_msg_seq_num" , "opt_c_sending_time" ,
               "opt_c_bid_price" , "opt_c_bid_size" ,
               "opt_c_bid_level" , "opt_c_offer_price" ,
               "opt_c_offer_size" , "opt_c_offer_level"]
    for col in columns:
        dd[col] = __np__.nan
    return dd


def search_topbook( futures=None ,
                    options=None ,
                    timestamp=None ,
                    month_codes=None ,
                    contract_ids=None ,
                    book_level=1 ,
                    standalone=False ):
    """
    Search pandas dataframe for specific timestamp
    :param contract_ids:
    :type month_codes: object
    :param standalone:
    :param book_level: level of books to search
    :param futures: Order book dataframe for futures contracts
    :param options: Order book for all options contracts
    :param timestamp: Timestamp to search in the order books
    :param month_codes: The codes to corresponding months. CME default "F,G,H,J,K,M,N,Q,U,V,X,Z"
    :return: Dictionary with the result of the timestamp search
    """
    if standalone:
        futures = futures[futures['bid_level'] == book_level]
        futures = futures[futures['security_desc'] != 'nan']
        futures = futures[~__np__.isnan(futures['bid_price'])]
        futures = futures[~__np__.isnan(futures['offer_price'])]
        options = options[options['bid_level'] == book_level]
        options = options[~__np__.isnan(options['bid_price'])]
        options = options[~__np__.isnan(options['offer_price'])]
        options = options[options['security_desc'] != 'nan']
        contract_ids = set(options['security_id'])

    if month_codes is None:
        month_codes = "F,G,H,J,K,M,N,Q,U,V,X,Z"
    month_codes = month_codes.lower()
    table = {"fut": []}
    codes = {k[1]: k[0] for k in enumerate(month_codes.rsplit(",") , 1)}
    fut_dict = {k: futures[k].item() for k in futures.dtype.names}
    for item in [fut_dict]:
        dd = __bookdict__(item , codes)
        table["fut"].append(dd.copy())
    for sec in contract_ids:
        items = options[options['security_id'] == sec]
        item = {k: items[-1][k].item() for k in items[-1].dtype.names}
        sec_desc = item['security_desc']
        price = int(sec_desc.split(" ")[1][1:])
        if price not in table.keys():
            table[price] = {i: {} for i in range(book_level)}
            dd = __orderdict__(item , codes)
            table_dd = table["fut"][book_level - 1].copy()
            table_dd.update(dd)
            table[price][book_level - 1] = table_dd.copy()
        else:
            dd = __orderdict__(item , codes)
            table[price][book_level - 1].update(dd)
        mask = __np__.isin(options , items)
        options = options[mask]
    del table["fut"]
    time_str = str(timestamp)
    datetime = Timestamp(year=int(time_str[0:4]) , month=int(time_str[4:6]) , day=int(time_str[6:8]) ,
                         hour=int(time_str[8:10]) , minute=int(time_str[10:12]) , second=int(time_str[12:14]) ,
                         microsecond=int(time_str[14:]) * 1000 , unit="ms").ceil("H")
    for key in table.keys():
        opt_p_sending_time = table[key][book_level - 1]['opt_p_sending_time']
        opt_p_sending_time = [str(i) if str(i) != 'nan' else i for i in [opt_p_sending_time]][0]
        table[key][book_level - 1]['opt_p_sending_time'] = opt_p_sending_time
        opt_c_sending_time = table[key][book_level - 1]['opt_c_sending_time']
        opt_c_sending_time = [str(i) if str(i) != 'nan' else i for i in [opt_c_sending_time]][0]
        table[key][book_level - 1]['opt_c_sending_time'] = opt_c_sending_time
        fut_sending_time = table[key][book_level - 1]['fut_sending_time']
        fut_sending_time = [str(i) if str(i) != 'nan' else i for i in [fut_sending_time]][0]
        table[key][book_level - 1]['fut_sending_time'] = fut_sending_time
        table[key][book_level - 1]['timestamp'] = time_str
        table[key][book_level - 1]['datetime'] = datetime
        table[key][book_level - 1]['date'] = datetime.date()
        table[key][book_level - 1]['year'] = datetime.year
        table[key][book_level - 1]['month'] = datetime.month_name()
        table[key][book_level - 1]['day'] = datetime.day_name()
        table[key][book_level - 1]['hour'] = datetime.hour
    return table


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
