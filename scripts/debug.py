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
import sys
from collections import defaultdict
import argparse


def _set_desc( security_desc ):
    global __securityDesc__
    __securityDesc__ = security_desc


def line_filter( line ):
    valid_contract = [sec if sec in line else None for sec in __securityDesc__]
    set_ids = iter(filter(None , valid_contract))
    security_ids = set(int(sec.split(b'\x0148=')[1].split(b'\x01')[0]) for sec in set_ids)
    if b'35=X\x01' in line and any(valid_contract):
        return security_ids , line


def line_map( line ):
    valid_contract = [sec if sec in line else None for sec in __securityDesc__]
    set_ids = iter(filter(None , valid_contract))
    security_ids = [int(sec.split(b'\x0148=')[1].split(b'\x01')[0]) for sec in set_ids]
    if any(valid_contract):
        pairs = {secid: line for secid in security_ids}
        return pairs


def __write__( security_id ):
    sec_desc = __securities__[security_id]
    product = ["opt" if len(sec_desc) > 7 else "fut"][0]
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


def __globaldict( msgs , filtered ):
    for item in iter(filter(None , filtered)):
        for key in item.keys():
            if not msgs.has_key(key):
                msgs[key] = []
                msgs[key] = msgs[key] + [item[key]]
            else:
                msgs[key] = msgs[key] + [item[key]]


def __msgsfilter__( line ):
    valid_contract = [sec if sec in line else None for sec in __securityDesc__]
    set_ids = iter(filter(None , valid_contract))
    security_ids = [int(sec.split(b'\x0148=')[1].split(b'\x01')[0]) for sec in set_ids]
    if any(valid_contract):
        pairs = {secid: line for secid in security_ids}
        return pairs


def data_filter( data=None , contract_ids=None , processes=None , chunksize=None ):
    security_desc = [b'\x0148=' + str(sec_id).encode() + b'\x01' for sec_id in contract_ids]
    if sys.version_info[0] > 2.7:
        msgs = defaultdict(list)
        with __mp__.Pool(initializer=_set_desc , initargs=(security_desc ,)) as pool:
            filtered = pool.map(__msgsfilter__ , data , chunksize)
            for item in iter(filter(None , filtered)):
                for key in item.keys():
                    msgs[key].append(item[key])
    else:
        # msgs = {}
        msgs = __mp__.Manager().dict()
        pool = __mp__.Pool(processes=processes , initializer=_set_desc , initargs=(security_desc ,))
        filtered = pool.map(__msgsfilter__ , data , chunksize)
        pool.close()
        p = __mp__.Process(target=__globaldict , args=(msgs , filtered))
        p.start()
        p.join()
        """
        for item in iter(filter(None , filtered)):
            for key in item.keys():
                if key not in msgs.keys():
                    msgs[key] = []
                    msgs[key].append(item[key])
                else:
                    msgs[key].append(item[key])
        pool.close()
        """
    try:
        data.close()
    except AttributeError:
        pass
    return msgs


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--file' , dest='file_path' , help='Fix data file input')
    parser.add_argument('--path_out' , dest='path_out' , help='Fix data file output')
    parser.add_argument('--year_code' , dest='year_code' , help='Fix data year code')
    parser.add_argument('--data_out' , dest='data_out' , help='Fix books path out')
    parser.add_argument('--compression', dest='compression', action='store_true')
    parser.add_argument('--no-compression', dest='compression', action='store_false')
    parser.add_argument('--process' , dest='process' , help='Number of threads')
    parser.add_argument('--book_process' , dest='book_process' , help='Number of threads')
    parser.add_argument('--chunksize' , dest='chunksize' , help='Data chunksize')
    parser.add_argument('--line_filter' , dest='func_parallel' , action='store_const' ,
                        const=line_filter , help='Function to return a tuple (security_ids,line)')
    parser.add_argument('--line_map' , dest='func_parallel' , action='store_const' ,
                        const=line_map , help='Function to return a dict {secs:line}')
    parser.set_defaults(compression=True)
    args = parser.parse_args()
    fixdata = fx.open_fix(path=args.file_path , compression=args.compression)
    data_lines = []
    for n,line in enumerate(fixdata.data):
        if n >=10000:
            break
        data_lines.append(line)
    fixdata.data.seek(0)
    opt_code = fx.most_liquid(data_line=data_lines[0] , instrument="ES" , product="OPT" , code_year=args.year_code)
    fut_code = fx.most_liquid(data_line=data_lines[0] , instrument="ES" , product="FUT" , code_year=args.year_code)
    liquid_secs = fx.liquid_securities(data_lines , code_year=args.year_code)
    contract_ids = liquid_secs.keys()
    security_desc = [b'\x0148=' + str(sec_id).encode() + b'\x01' for sec_id in contract_ids]
    start = time.time()
    contracts = data_filter(fixdata.data , contract_ids , int(args.process) , int(args.chunksize))
    end = time.time()

    print("total_time \t threads \t  chunksize")
    print(str(end - start) + "\t" + args.process + "\t" + args.chunksize)
    #print(contract_ids)
    #print(contracts.keys())
    #print([len(contracts[k]) for k in contracts.keys()])

"""

    start = time.time()
    pool = __mp__.Pool(processes=int(args.process), initializer=set_secdesc, initargs=(security_desc,))
    result = pool.map(args.func_parallel, fixdata.data, int(args.chunksize))
    pool.close()
    end = time.time()
    
    print("total_time \t threads \t  chunksize")
    print(str(end - start) + "\t" + args.process + "\t" + args.chunksize)
    print(next(iter(filter(None, result))))
    
#python debug.py --file "/home/cme/data/xcme/2010/XCME_MD_ES_20091228_20100101" --year_code 0 --process 40 --chunksize 31 --line_map

#python debug.py --file "/home/cme/data/xcme/2010/XCME_MD_ES_20091228_20100101" 
# --path_out "/home1/05191/jlroo/cme/books/" --year_code 0 --process 72 --chunksize 32 --book_process 12

#python debug.py --file "/home/cme/data/xcme/2010/XCME_MD_ES_20091228_20100101" --year_code 0 --process 40 --chunksize 32 --book_process 12

"""
