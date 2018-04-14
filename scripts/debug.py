# -*- coding: utf-8 -*-
"""
Created on Sun Mar 25 16:44:25 2018

@author: jrodriguezorjuela
"""

import fixtools as fx
from fixtools.core.book import OrderBook
from collections import defaultdict
import multiprocessing as __mp__


def init_filter( __security_desc__ ):
    global security_desc
    security_desc = __security_desc__


def __filter__( line ):
    valid_contract = [sec if sec in line else None for sec in security_desc]
    set_ids = filter(None , valid_contract)
    security_ids = set(int(sec.split(b'\x0148=')[1].split(b'\x01')[0]) for sec in set_ids)
    if b'35=X\x01' in line and any(valid_contract):
        return security_ids , line


def data_filter( data , contract_ids , path , chunksize ):
    msgs = defaultdict(list)
    security_desc = [b'\x0148=' + str(sec_id).encode() + b'\x01' for sec_id in contract_ids]
    with __mp__.Pool(initializer=init_filter , initargs=(security_desc ,)) as pool:
        filtered = pool.map(__filter__ , data , chunksize)
        try:
            data.close()
        except AttributeError:
            del data
    for set_ids , line in filter(None , filtered):
        for security_id in set_ids:
            msgs[security_id].append(line)
            with open(path + str(security_id) , 'ab+') as secdata:
                secdata.write(line)
    return msgs


def write_filter( data , contract_ids , path , chunksize ):
    # msgs = defaultdict(list)
    security_desc = [b'\x0148=' + str(sec_id).encode() + b'\x01' for sec_id in contract_ids]
    with __mp__.Pool(initializer=init_filter , initargs=(security_desc ,)) as pool:
        filtered = pool.map(__filter__ , data , chunksize)
    for set_ids , line in filter(None , filtered):
        for security_id in set_ids:
            with open(path + str(security_id) , 'ab+') as secdata:
                secdata.write(line)


def data_book( data , securities , path="" , chunksize=10 ** 4 ):
    contract_ids = set(securities.keys())
    contracts = data_filter(data , contract_ids , path , chunksize)
    if path != "":
        for security_id in contract_ids:
            sec_desc = securities[security_id]
            product = ["opt" if len(sec_desc) > 5 else "fut"][0]
            book_obj = OrderBook(contracts[security_id] , security_id , product)
            filename = sec_desc.replace(" " , "-")
            with open(path + filename , 'ab+') as book_out:
                for book in book_obj.build_book():
                    book_out.write(book)


if __name__ == "__main__":
    # path = "E:/analyticslab/2010M/XCME_MD_ES_20100412_20100416.gz"
    # path = "E:/analyticslab/2010M/XCME_MD_ES_20100503_20100507.gz
    # path = "E:/analyticslab/2010M/XCME_MD_ES_20100517_20100521.gz
    path = "E:/analyticslab/2010M\XCME_MD_ES_20100531_20100604.gz"

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
    filename = fut_code[2] + opt_code[2] + "-"
    path_out = desc_path + filename
    chunksize = 32000

    # data_book(data=fixdata.data , securities=securities , path=path_out , chunksize=chunksize)
    contract_ids = set(securities.keys())
    write_filter(fixdata.data , contract_ids , path_out , chunksize)
