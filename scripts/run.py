#!/opt/anaconda3/bin/python

"""
 Created on Wed Jul 20 11:24:41 2016
 @author: jlroo

 ls *.gz | parallel "gunzip -c {} | bzip2 > {.}.bz2"

time ./run.py --path /home/jlroo/cme/data/raw/test/

"""

import fixtools as fx
import pandas as pd

def main():

    path = "/home/jlroo/cme/data/output/"
    out_query = "/home/jlroo/cme/data/search/"
    fixfiles = fx.files_tree(path)
    sending_time = "210000000"

    for key in fixfiles.keys():
        opt_file = fixfiles[key]['options']
        options = pd.read_csv(path+opt_file[0])
        fut_file = fixfiles[key]['futures'][0]
        futures = pd.read_csv(path+fut_file)
        trade_dates = list(futures['trade_date'].unique())
        for date in trade_dates:
            timestamp = str(date) + sending_time
            result = fx.put_call_query(futures, options, timestamp)
            fx.search_out(result, timestamp, out_query)
            print("[DONE] -- FUT -- " + fut_file + " -- " + timestamp)

if __name__ == "__main__":
    main()
