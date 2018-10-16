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

    path = "/home/cme/2010/output/"
    path_out = "/home/cme/2010/parity/"
    path_rates = "/home/cme/2010/rates/tbill-rates.csv"
    rates_table = pd.read_csv(path_rates)

    columns = ['share_strike' ,
               'put_call' ,
               'share_pv_strike' ,
               'put_call_diff' ,
               'strike_price' ,
               'trade_date' ,
               'exp_date' ,
               'exp_days' ,
               'fut_offer_price' ,
               'fut_bid_price' ,
               'opt_p_bid_price' ,
               'opt_c_bid_price' ,
               'opt_p_offer_price' ,
               'opt_c_offer_price' ,
               'fut_bid_size' ,
               'fut_offer_size' ,
               'opt_p_bid_size' ,
               'opt_c_bid_size' ,
               'opt_p_offer_size' ,
               'opt_c_offer_size']

    fx.weekly_liquidity(path=path , path_out=path_out ,
                        df_rates=rates_table , columns=columns ,
                        method='minute' , chunksize=25600)
