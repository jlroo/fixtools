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
    src = "/home/cme/exchange_XCME/year_2010/asset_EQUITY/product_ES/"
    path_files = src + "md_BOOKS/month_M/"
    path_out = src + "md_BOOKS_TOP/month_M/"
    path_times = src + "md_BOOKS_TIMESTAMP/month_M/"
    path_rates = src + "algorithms/liquidity/data/tbill_rates.csv"
    rates_table = pd.read_csv(path_rates)

    fx.weekly_liquidity(path_files=path_files ,
                        path_out=path_out ,
                        path_times=path_times ,
                        df_rates=rates_table ,
                        frequency='minute' ,
                        chunksize=25600)
