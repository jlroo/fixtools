#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Sun Mar 25 16:44:25 2018

@author: jrodriguezorjuela
"""

import fixtools as fx
import multiprocessing as __mp__


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
        pairs = [(id , line) for id in security_ids]
        return pairs

if __name__ == "__main__":
    path = "/work/05191/jlroo/stampede2/2010/XCME_MD_ES_20091207_2009121"
    fixdata = fx.open_fix(path , compression=False)
    data_lines = fixdata.data.readlines(10000)
    fixdata.data.seek(0)
    year_code = "0"
    opt_code = fx.most_liquid(data_line=data_lines[0] , instrument="ES" , product="OPT" , code_year=year_code)
    fut_code = fx.most_liquid(data_line=data_lines[0] , instrument="ES" , product="FUT" , code_year=year_code)
    liquid_secs = fx.liquid_securities(data_lines , code_year=year_code)
    contract_ids = set(liquid_secs.keys())
    security_desc = [b'\x0148=' + str(sec_id).encode() + b'\x01' for sec_id in contract_ids]

    ## SKX Compute Nodes
    ## 48 cores on two sockets (24 cores/socket)
    ## Cache: 	32KB L1 data cache per core;
    ## 1MB L2 per core; 33MB L3 per socket.
    ## Each socket can cache up to 57MB (sum of L2 and L3 capacity).

    pool = __mp__.Pool(processes=24 , initializer=set_secdesc , initargs=(security_desc ,))
    result = pool.map(line_filter , fixdata.data , 32000)
    pool.close()
