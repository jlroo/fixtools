#!/usr/bin/env python3

"""
 Created on Wed Jul 20 11:24:41 2016
 @author: jlroo

 ls *.gz | parallel "gunzip -c {} | bzip2 > {.}.bz2"

time ./run.py --path /home/jlroo/cme/data/raw/test/

"""

import fixtools as fx
import pandas as pd



def search_fix(query=False):
    path_books = "/home/jlroo/data/pipeline/2010/M/"
    out_table = "/home/jlroo/data/output/"
    out_query = "/home/jlroo/data/search/"
    path_rates = "/home/jlroo/data/rates/"
    
    fixfiles = fx.files_tree(path_books)

    for key in fixfiles.keys():

        opt_files = fixfiles[key]['options']
        options = fx.options_table(path_books,
                                   opt_files,
                                   num_orders = 1,
                                   write_csv = True,
                                   path_out = out_table,
                                   return_table = True)

        fut_file = fixfiles[key]['futures'][0]
        futures = fx.futures_table(path_books,
                                   fut_file,
                                   num_orders = 1,
                                   write_csv = True,
                                   path_out = out_table,
                                   return_table = True)

        times = fx.time_table(futures, options)

        columns = ['share_strike','put_call','share_pv_strike',
                   'put_call_diff','strike_price','trade_date',
                   'exp_date', 'exp_days',
                   'fut_offer_price','fut_bid_price',
                   'opt_p_bid_price', 'opt_c_bid_price',
                   'opt_p_offer_price', 'opt_c_offer_price',
                   'fut_bid_size', 'fut_offer_size', 
                   'opt_p_bid_size', 'opt_c_bid_size', 
                   'opt_p_offer_size', 'opt_c_offer_size']

        rates_table = pd.read_csv(path_rates + "tbill-2010.csv")

        for date in times['futures'].keys():
            for hour in times['futures'][date].keys():
                timestamp = str(times['futures'][date][hour][-1])
                if query:
                    result = fx.put_call_query(futures, options, timestamp)
                else:
                    result = fx.put_call_parity(futures, options, rates_table, timestamp)
                if not result =={}:
                    fx.search_out(result, timestamp,out_query, ordered = columns)
                    print("[DONE] -- FUT -- " + fut_file + " -- " + timestamp)

if __name__ == "__main__":
    #search_csv()
    search_fix()

