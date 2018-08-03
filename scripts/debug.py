#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Sun Mar 25 16:44:25 2018

@author: jrodriguezorjuela
"""

import fixtools as fx
from fixtools import OrderBook
import multiprocessing as __mp__
import time
from multiprocessing.pool import ThreadPool
import argparse


def set_secdesc( security_desc ):
    global __securityDesc__
    __securityDesc__ = security_desc


def line_filter( line ):
    valid_contract = [sec if sec in line else None for sec in __securityDesc__]
    set_ids = iter(filter(None , valid_contract))
    security_ids = set(int(sec.split(b'\x0148=')[1].split(b'\x01')[0]) for sec in set_ids)
    if b'35=X\x01' in line and any(valid_contract):
        return (security_ids , line)


def line_map( line ):
    valid_contract = [sec if sec in line else None for sec in __securityDesc__]
    set_ids = iter(filter(None , valid_contract))
    security_ids = [int(sec.split(b'\x0148=')[1].split(b'\x01')[0]) for sec in set_ids]
    if any(valid_contract):
        pairs = {secid: line for secid in security_ids}
        return pairs


def __write__( security_id ):
    sec_desc = __securities__[security_id]
    product = ["opt" if len(sec_desc) < 7 else "fut"][0]
    book_obj = OrderBook(__contracts__[security_id] , security_id , product)
    filename = __securities__[security_id].replace(" " , "-")
    with open(__path__ + filename , 'ab+') as book_out:
        for book in book_obj.build_book():
            book_out.write(book)


def _set_writes( securities , contracts , path ):
    global __path__
    __path__ = path
    global __contracts__
    __contracts__ = contracts
    global __securities__
    __securities__ = securities



if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--file' , dest='file_path' , help='Fix data file input')
    parser.add_argument('--path_out' , dest='path_out' , help='Fix data file output')
    parser.add_argument('--year_code' , dest='year_code' , help='Fix data year code')
    parser.add_argument('--data_out' , dest='data_out' , help='Fix books path out')
    parser.add_argument('--compression' , dest='compression' , help='Compression (False default)')
    parser.add_argument('--process' , dest='process' , help='Number of threads')
    parser.add_argument('--book_process' , dest='process' , help='Number of threads')
    parser.add_argument('--chunksize' , dest='chunksize' , help='Data chunksize')
    parser.add_argument('--line_filter' , dest='func_parallel' , action='store_const' ,
                        const=line_filter , help='Function to return a tuple (security_ids,line)')
    parser.add_argument('--line_map' , dest='func_parallel' , action='store_const' ,
                        const=line_map , help='Function to return a dict {secs:line}')

    args = parser.parse_args()

    compression = False
    if args.compression:
        compression = True

    fixdata = fx.open_fix(path=args.file_path , compression=compression)
    data_lines = fixdata.data.readlines(10000)
    fixdata.data.seek(0)
    opt_code = fx.most_liquid(data_line=data_lines[0] , instrument="ES" , product="OPT" , code_year=args.year_code)
    fut_code = fx.most_liquid(data_line=data_lines[0] , instrument="ES" , product="FUT" , code_year=args.year_code)
    liquid_secs = fx.liquid_securities(data_lines , code_year=args.year_code)
    contract_ids = liquid_secs.keys()
    security_desc = [b'\x0148=' + str(sec_id).encode() + b'\x01' for sec_id in contract_ids]

    contracts = fx.data_filter(fixdata.data , contract_ids , args.process , args.chunksize)

    # pool = ThreadPool()

    pool = __mp__.Pool(processes=int(args.book_process) , initializer=_set_writes ,
                       initargs=(liquid_secs , contracts , args.path_out))
    pool.map(__write__ , contract_ids)
    pool.close()

"""

    start = time.time()
    pool = __mp__.Pool(processes=int(args.process) , initializer=set_secdesc , initargs=(security_desc ,))
    result = pool.map(args.func_parallel , fixdata.data , int(args.chunksize))
    pool.close()
    end = time.time()
    
    print("total_time \t threads \t  chunksize")
    print(str(end - start) + "\t" + args.process + "\t" + args.chunksize)
    print(next(iter(filter(None , result))))

# python debug.py --file "/work/05191/jlroo/stampede2/2010/XCME_MD_ES_20091207_2009121" 
# --year_code 0 --process 72 --chunksize 3000 --line_filter
    compression = False
    file_path = "/work/05191/jlroo/stampede2/2010/XCME_MD_ES_20091207_2009121"
    year_code = "0"
    chunksize = 3000
    fixdata = fx.open_fix(path=file_path , compression=compression)
    data_lines = fixdata.data.readlines(10000)
    fixdata.data.seek(0)
    opt_code = fx.most_liquid(data_line=data_lines[0] , instrument="ES" , product="OPT" , code_year=year_code)
    fut_code = fx.most_liquid(data_line=data_lines[0] , instrument="ES" , product="FUT" , code_year=year_code)
    liquid_secs = fx.liquid_securities(data_lines, code_year=year_code)
    contract_ids = set(liquid_secs.keys())
    security_desc = [b'\x0148=' + str(sec_id).encode() + b'\x01' for sec_id in contract_ids]
    
    process = 72
    chunksize = 1200
    pool = __mp__.Pool(processes=process, initializer=set_secdesc, initargs=(security_desc,))
    result = pool.map(line_filter, fixdata.data, chunksize)
    pool.close()
    
# python debug.py --file "/work/05191/jlroo/stampede2/2010/XCME_MD_ES_20091207_2009121"
# --year_code 0 --process 72 --chunksize 3000 --line_map

"""
