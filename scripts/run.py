#!/home/jlroo/anaconda3/bin/python

"""
 Created on Wed Jul 20 11:24:41 2016
 @author: jlroo

 ls *.gz | parallel "gunzip -c {} | bzip2 > {.}.bz2"

time ./run.py --path /home/jlroo/cme/data/raw/test/

"""

import fixtools as fx
import pandas as pd


def search_csv():
    path = "/home/jlroo/cme/data/output/"
    out_query = "/home/jlroo/cme/data/search/"
    fixfiles = fx.files_tree(path)
    for key in fixfiles.keys():
        opt_file = fixfiles[key]['options'][0]
        options = pd.read_csv(path+opt_file)
        fut_file = fixfiles[key]['futures'][0]
        futures = pd.read_csv(path+fut_file)
        times = fx.time_table(futures, options)
        for date in times['futures'].keys():
            for hour in times['futures'][date].keys():
                timestamp = str(times['futures'][date][hour][-1])
                result = fx.put_call_query(futures, options, timestamp)
                if not result =={}:
                    fx.search_out(result, timestamp, out_query)
                    print("[DONE] -- FUT -- " + fut_file + " -- " + timestamp)



def search_fix():
    path = "/data/cme/2010/H/"
    out_table = "/home/jlroo/cme/data/output/"
    out_query = "/home/jlroo/cme/data/search/"
    fixfiles = fx.files_tree(path)

    for key in fixfiles.keys():

        opt_files = fixfiles[key]['options']
        options = fx.options_table(path,
                                   opt_files,
                                   num_orders = 1,
                                   write_csv = True,
                                   path_out = out_table,
                                   return_table = True)

        fut_file = fixfiles[key]['futures'][0]
        futures = fx.futures_table(path,
                                   fut_file,
                                   num_orders = 1,
                                   write_csv = True,
                                   path_out = out_table,
                                   return_table = True)

        times = fx.time_table(futures, options)

        for date in times['futures'].keys():
            for hour in times['futures'][date].keys():
                timestamp = str(times['futures'][date][hour][-1])
                result = fx.put_call_query(futures, options, timestamp)
                if not result=={}:
                    fx.search_out(result, timestamp, out_query)
                    print("[DONE] -- FUT -- " + fut_file + " -- " + timestamp)

if __name__ == "__main__":
    #search_csv()
    search_fix()

