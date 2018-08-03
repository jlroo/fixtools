#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Sun Mar 25 16:44:25 2018

@author: jrodriguezorjuela
"""

import fixtools as fx
import multiprocessing as __mp__
import time
import argparse


def set_secdesc( security_desc ):
    global __securityDesc__
    __securityDesc__ = security_desc


def line_filter( line ):
    valid_contract = [sec if sec in line else None for sec in security_desc]
    set_ids = filter(None , valid_contract)
    security_ids = set(int(sec.split(b'\x0148=')[1].split(b'\x01')[0]) for sec in set_ids)
    if b'35=X\x01' in line and any(valid_contract):
        return (security_ids , line)


def line_map( line ):
    valid_contract = [sec if sec in line else None for sec in security_desc]
    set_ids = filter(None , valid_contract)
    security_ids = [int(sec.split(b'\x0148=')[1].split(b'\x01')[0]) for sec in set_ids]
    if any(valid_contract):
        pairs = {secid: line for secid in security_ids}
        return pairs


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--file' , dest='file_path' , help='Fix data file input')
    parser.add_argument('--year_code' , dest='year_code' , help='Fix data year code')
    parser.add_argument('--data_out' , dest='data_out' , help='Fix books path out')
    parser.add_argument('--compression' , dest='compression' , help='Compression (False default)')
    parser.add_argument('--process' , dest='chunksize' , help='Number of threads')
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
    contract_ids = set(liquid_secs.keys())
    security_desc = [b'\x0148=' + str(sec_id).encode() + b'\x01' for sec_id in contract_ids]

    start = time.time()
    pool = __mp__.Pool(processes=int(args.process) , initializer=set_secdesc , initargs=(security_desc ,))
    result = pool.map(args.func_parallel , fixdata.data , int(args.chunksize))
    pool.close()
    end = time.time()

    print("\t total_time \t threads \t  chunksize" + "\n")
    print("\t" + str(end - start) + "\t" + args.process + "\t" + args.chunksize + "\n")
