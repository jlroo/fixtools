#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Created on Sun Nov  5 09:53:03 2017

@author: jlroo
"""

import fixtools as fx
import datetime as __datetime__
import pandas as __pd__
import numpy as __np__
import multiprocessing as __mp__
from collections import defaultdict
from fixtools.util.util import expiration_date,  open_fix
from fixtools.io.fixfast import FixDict

def book_table(path=None, path_out=None, filename=None, product="futures|options", num_orders=1, chunksize=32000):
    if path[-1] != "/":
        path = path + "/"
    dfs = []
    files = [filename]
    for item in files:
        file_path = path + item
        fixdata = open_fix(file_path, compression=False)
        fix_dict = FixDict(num_orders = num_orders)
        with __mp__.Pool() as pool:
            df = pool.map(fix_dict.to_dict, fixdata.data, chunksize=chunksize)
            dfs.append(__pd__.DataFrame.from_dict(df))
        try:
            fixdata.data.close()
        except AttributeError:
            pass
    contract_book = __pd__.concat(dfs)
    contract_book = contract_book.replace('NA' , __np__.nan)
    if path_out:
        if path_out[-1] != "/":
            path_out = path_out + "/"
        if product in "opt|options":
            file_name = path_out + files[0][0][:-5] + "OPTIONS.csv"
        elif product in "fut|futures":
            file_name = path_out + files[0][0] + ".csv"
        contract_book.to_csv(file_name , index=False)  
    return contract_book

def main():
    path_books = "/run/media/analyticslab/INTEL SSD/analyticslab/pipeline-2009/2009/M/"
    out_table = "/home/analyticslab/cme-data/output/"
    out_query = "/home/analyticslab/cme-data/parity/"
    path_rates = "/home/analyticslab/cme-data/rates/tbill-rates.csv"
    rates_table = __pd__.read_csv(path_rates)
    fixfiles = fx.files_tree(path_books)
    chunksize = 32000
    num_orders = 1

    for key in fixfiles.keys():
        files = fixfiles[key]['options']
        dfs = []
        for filename in files:
            path_file = path_books + filename
            fixdata = open_fix(path_file, compression=False)
            fix_dict = FixDict(num_orders)
            with __mp__.Pool() as pool:
                df = pool.map(fix_dict.to_dict, fixdata.data, chunksize=chunksize)
                dfs.append(__pd__.DataFrame.from_dict(df))
            try:
                fixdata.data.close()
            except AttributeError:
                pass
        options = __pd__.concat(dfs)
        options = options.replace('NA' , __np__.nan)

if __name__ == '__main__':
    main()



