#!/usr/bin/env python3

"""
 Created on Wed Jul 20 11:24:41 2016
 @author: jlroo

 ls *.gz | parallel "gunzip -c {} | bzip2 > {.}.bz2"

time ./run.py --path /home/jlroo/cme/data/raw/test/

"""

import fixtools as fx
import pandas as pd


def put_call_csv():
    path = "/home/jlroo/data/output/"
    out_query = "/home/jlroo/data/parity/"
    rates_table = pd.read_csv("/home/jlroo/data/rates/tbill-2010.csv")
    fixfiles = fx.files_tree(path)

    columns = ['share_strike' , 'put_call' , 'share_pv_strike' ,
               'put_call_diff' , 'strike_price' , 'trade_date' ,
               'exp_date', 'exp_days',
               'fut_offer_price' , 'fut_bid_price' ,
               'opt_p_bid_price', 'opt_c_bid_price',
               'opt_p_offer_price', 'opt_c_offer_price',
               'fut_bid_size', 'fut_offer_size', 
               'opt_p_bid_size', 'opt_c_bid_size', 
               'opt_p_offer_size', 'opt_c_offer_size']
    
    for key in fixfiles.keys():
        opt_file = fixfiles[key]['options'][0]
        options = pd.read_csv(path+opt_file)
        fut_file = fixfiles[key]['futures'][0]
        futures = pd.read_csv(path+fut_file)
        times = fx.time_table(futures=futures , options=options , chunksize=48000)
        for date in times['futures'].keys():
            for hour in times['futures'][date].keys():
                timestamp = str(times['futures'][date][hour][-1])
                result = fx.put_call_parity(futures=futures , options=options , rates_table=rates_table , timestamp=timestamp , level_limit=1)
                if not result == {}:
                    fx.search_out(result=result , timestamp=timestamp , path_out=out_query , ordered=columns)
                    print("[DONE] -- FUT -- " + fut_file + " -- " + timestamp)


if __name__ == "__main__":
    put_call_csv()
