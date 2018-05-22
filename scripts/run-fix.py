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

    #path = "/run/media/analyticslab/INTEL SSD/analyticslab/pipeline-2009/2009/M/"
    #path = "/run/media/analyticslab/INTEL SSD/analyticslab/pipeline-2009/2009/Z/"
    path = "/run/media/analyticslab/2EB4D45BB4D4275D/cme-data/U/"
    path_out = "/run/media/analyticslab/INTEL SSD/analyticslab/output/"
    path_search = "/run/media/analyticslab/INTEL SSD/analyticslab/parity/"
    rates = "/run/media/analyticslab/INTEL SSD/analyticslab/rates/tbill-rates.csv"
    rates_table = pd.read_csv(rates)
    columns = ['share_strike','put_call','share_pv_strike','put_call_diff','strike_price','trade_date',
        'exp_date', 'exp_days','fut_offer_price','fut_bid_price','opt_p_bid_price', 'opt_c_bid_price',
        'opt_p_offer_price', 'opt_c_offer_price','fut_bid_size', 'fut_offer_size', 'opt_p_bid_size', 
        'opt_c_bid_size', 'opt_p_offer_size', 'opt_c_offer_size']    
    fx.search_fix(  path = path, 
                    path_out = path_out,
                    path_search = path_search,
                    df_rates = rates_table,  
                    columns = columns,
                    num_orders = 1,
                    chunksize = 48000,
                    read_ram = True)

