#!/usr/bin/env python3

"""
 Created on Wed Jul 20 11:24:41 2016
 @author: jlroo

 ls *.gz | parallel "gunzip -c {} | bzip2 > {.}.bz2"

time ./run.py --path /home/jlroo/cme/data/raw/test/

"""

import fixtools as fx

if __name__ == "__main__":
    path_files = "/home/cme/exchange_XCME/year_2010/asset_EQUITY/product_ES/md_BOOKS/month_H/"
    path_out = "/home/cme/exchange_XCME/year_2010/asset_EQUITY/product_ES/md_BOOKS_TOP/month_H/"
    path_times = "/home/cme/exchange_XCME/year_2010/asset_EQUITY/product_ES/md_BOOKS_TIMESTAMP/month_H/"

    dtype = {'names': ['msg_seq_num' , 'security_id' , 'security_desc' , 'sending_time' , 'trade_date ' ,
                       'bid_price' , 'bid_size' , 'bid_level' , 'offer_price' , 'offer_size' , 'offer_level'] ,
             'formats': ['>i4' , '<U25' , '<U25' , '>i8' , '>i4' , '>f4' , '>f4' , '>i2' , '>f4' , '>f4' , '>i2']}

    fx.weekly_orderbooks(path_files=path_files , path_out=path_out , path_times=path_times ,
                         num_orders=1 , chunksize=25600 , read_ram=True , dtype=dtype)
