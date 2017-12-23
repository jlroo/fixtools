#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Created on Sun Nov  5 09:53:03 2017

@author: jlroo
"""

import fixtools as fx

path = "/home/jlroo/cme/data/books/2010/M/"
out_table = "/home/jlroo/cme/data/output/"
out_query = "/home/jlroo/cme/data/search/"
fixfiles = fx.files_tree(path)
sending_time = "210000000"

for key in fixfiles.keys():

    opt_files = fixfiles[key]['options']
    options = fx.options_table(path,
                               opt_files,
                               num_orders=1,
                               write_csv=True,
                               path_out=out_table,
                               return_table=True)

    fut_file = fixfiles[key]['futures'][0]
    futures = fx.futures_table(path,
                               fut_file,
                               num_orders = 1,
                               write_csv = True,
                               path_out = out_table,
                               return_table = True)

    trade_dates = list(futures['trade_date'].unique())

    for date in trade_dates:
        timestamp = str(date) + sending_time
        result = fx.put_call_query(futures, options, timestamp)
        fx.search_out(result, timestamp, out_query)



