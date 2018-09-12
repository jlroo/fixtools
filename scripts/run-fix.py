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
    path = "/home/cme/2010/pipeline/2010/H/"
    path_out = "/home/cme/2010/output/"
    path_times = "/home/cme/2010/times/"

    fx.search_fix( path=path, path_out=path_out, path_times=path_times, num_orders=1, chunksize=25600)

"""

## When using parity check True

path = "/home/cme/2010/pipeline/2010/H/"
path_out = "/home/cme/2010/output/"
path_parity = "/home/cme/2010/parity/"
path_times = "/home/cme/2010/times/"
path_rates = "/home/cme/2010/rates/tbill-rates.csv"
rates_table = pd.read_csv(path_rates)
columns = ['share_strike','put_call','share_pv_strike','put_call_diff','strike_price','trade_date',
    'exp_date', 'exp_days','fut_offer_price','fut_bid_price','opt_p_bid_price', 'opt_c_bid_price',
    'opt_p_offer_price', 'opt_c_offer_price','fut_bid_size', 'fut_offer_size', 'opt_p_bid_size', 
    'opt_c_bid_size', 'opt_p_offer_size', 'opt_c_offer_size']
fx.search_fix( path=path, path_out=path_out,
            path_parity=path_parity, path_times=path_times,
            df_rates=rates_table, columns=columns, 
            num_orders=1, chunksize=25600,
            read_ram=True, parity_check=False)

"""