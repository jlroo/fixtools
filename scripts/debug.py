# -*- coding: utf-8 -*-
"""
Created on Sun Mar 25 16:44:25 2018

@author: jrodriguezorjuela
"""

import fixtools as fx
from fixtools.core.book import OrderBook
from collections import defaultdict
import multiprocessing as __mp__

__path__ = None
__contracts__ = None
__securities__ = None
__securityDesc__ = None


class DataBook:
    global __path__
    global __contracts__
    global __securities__
    global __securityDesc__

    def __init__( self , data , securities , chunksize=32000 ):
        self.data = data
        self.chunksize = chunksize
        __securities__ = securities
        self.contract_ids = set(securities.keys())
        __securityDesc__ = [b'\x0148=' + str(sec_id).encode() + b'\x01' for sec_id in self.contract_ids]
        __contracts__ = self.filter()

    def filter( self ):
        messages = defaultdict(list)
        with __mp__.Pool() as pool:
            filtered = pool.map(__filter__ , self.data , self.chunksize)
            for set_ids , line in filter(None , filtered):
                for security_id in set_ids:
                    messages[security_id].append(line)
        try:
            self.data.seek(0)
        except AttributeError:
            pass
        return messages

    def create( self , path=None ):
        if path:
            __path__ = path
            with __mp__.Pool() as pool:
                pool.map(__write__ , self.contract_ids , self.chunksize)
        else:
            with __mp__.Pool() as pool:
                books = pool.map(__build__ , self.contract_ids , self.chunksize)
            return books


def __build__( security_id ):
    sec_desc = __securities__[security_id]
    product = ["opt" if len(sec_desc) < 7 else "fut"][0]
    books = []
    book_obj = OrderBook(__contracts__[security_id] , security_id , product)
    for book in book_obj.build_book():
        books.append(book)
    return {security_id: books}


def __write__( security_id ):
    sec_desc = __securities__[security_id]
    product = ["opt" if len(sec_desc) < 7 else "fut"][0]
    book_obj = OrderBook(__contracts__[security_id] , security_id , product)
    filename = __securities__[security_id].replace(" " , "-")
    with open(__path__ + filename , 'ab+') as book_out:
        for book in book_obj.build_book():
            book_out.write(book)


def __filter__( line ):
    valid_contract = [sec if sec in line else None for sec in __securityDesc__]
    set_ids = filter(None , valid_contract)
    security_ids = set(int(sec.split(b'\x0148=')[1].split(b'\x01')[0]) for sec in set_ids)
    if b'35=X\x01' in line and any(valid_contract):
        return security_ids , line


def set_secdesc( security_desc ):
    global __securityDesc__
    __securityDesc__ = security_desc


def data_filter( data , contract_ids , chunksize ):
    msgs = defaultdict(list)
    security_desc = [b'\x0148=' + str(sec_id).encode() + b'\x01' for sec_id in contract_ids]
    with __mp__.Pool(initializer=set_secdesc , initargs=(security_desc ,)) as pool:
        filtered = pool.map(__filter__ , data , chunksize)
        for set_ids , line in filter(None , filtered):
            for security_id in set_ids:
                msgs[security_id].append(line)
    try:
        data.close()
    except AttributeError:
        pass
    return msgs


def set_write( securities , contracts , path ):
    global __path__
    __path__ = path
    global __contracts__
    __contracts__ = contracts
    global __securities__
    __securities__ = securities


def data_book( data , securities , path=None , chunksize=32000 ):
    contract_ids = set(securities.keys())
    contracts = data_filter(data , contract_ids , chunksize)
    if path:
        __path__ = path
        with __mp__.Pool(initializer=set_write , initargs=(securities , contracts , path)) as pool:
            pool.map(__write__ , contract_ids , chunksize)
    else:
        with __mp__.Pool() as pool:
            books = pool.map(__build__ , contract_ids , chunksize)
        return books


path = "E:/analyticslab/2010M/XCME_MD_ES_20100412_20100416.gz"
# path = "E:/2010M/XCME_MD_ES_20100517_20100521.gz"
# path = "E:/2010M/XCME_MD_ES_20100531_20100604.gz"

fixdata = fx.open_fix(path=path)
dates = fixdata.dates
year_code = "0"
securities = fx.liquid_securities(fixdata , year_code=year_code)

opt_code = fx.most_liquid(dates=dates ,
                          instrument="ES" ,
                          product="OPT" ,
                          year_code=year_code)

fut_code = fx.most_liquid(dates=dates ,
                          instrument="ES" ,
                          product="FUT" ,
                          year_code=year_code)

data_out = "E:/analyticslab/pipeline/2010/"
desc_path = data_out + fut_code[2] + "/"
filename = "XCME" + "-" + fut_code[2] + opt_code[2] + "-"
path_out = desc_path + filename
chunksize = 38000

data_book(data=fixdata.data , securities=securities , path=path_out , chunksize=chunksize)
