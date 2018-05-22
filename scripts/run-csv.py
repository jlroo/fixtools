#!/usr/bin/env python3

"""
 Created on Wed Jul 20 11:24:41 2016
 @author: jlroo

 ls *.gz | parallel "gunzip -c {} | bzip2 > {.}.bz2"

time ./run.py --path /home/jlroo/cme/data/raw/test/

"""

import fixtools as fx
import pandas as pd

if __name__ == "__main__":
    path = "/home/jlroo/data/output/"
    path_out = "/home/jlroo/data/parity/"
    rates_table = pd.read_csv("/home/jlroo/data/rates/tbill-2010.csv")
    columns = ['share_strike','put_call','share_pv_strike','put_call_diff','strike_price','trade_date',
        'exp_date', 'exp_days','fut_offer_price','fut_bid_price','opt_p_bid_price', 'opt_c_bid_price',
        'opt_p_offer_price', 'opt_c_offer_price','fut_bid_size', 'fut_offer_size', 'opt_p_bid_size', 
        'opt_c_bid_size', 'opt_p_offer_size', 'opt_c_offer_size']
    fx.search_csv(  path = path,
                    path_out = path_out, 
                    df_rates = rates_table, 
                    columns = columns, 
                    chunksize = 48000)