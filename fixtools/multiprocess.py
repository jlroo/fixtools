#!/opt/anaconda3/bin/python

"""
Created on Mon Aug  8 11:56:15 2016

@author: jlroo
"""
import argparse
import bz2
import re
import pandas as pd
from collections import Counter
import multiprocessing as mp

def process(line):

    # GET SECURITY ID
    sec = re.search(b'(\x0148\=)(.*)(\x01)',line)
    sec = int(sec.group(2).split(b'\x01')[0])
    
    # GET SECURITY DESCRIPTION
    secdes = re.search(b'(\x01107\=)(.*)(\x01)',line)
    secdes = secdes.group(2).split(b'\x01')[0].decode()
    
    # GET SENDING DATE TAG 52
    day = line[line.find(b'\x0152=')+4:line.find(b'\x0152=')+12].decode()
    
    secinfo = {'desc':secdes,'sday':day}
    info = (sec,secinfo)

    return info

def report(result):
    volume = Counter(elem[0] for elem in result).most_common(10)
    info = []
    secinfo = dict((key, value) for (key, value) in result)
    
    for sec in volume:
        info.append({'SendingDate':secinfo[sec[0]]['sday'],
                         'SecurityID':str(sec[0]),
                         'SecurityDesc':secinfo[sec[0]]['desc'],
                         'Volume':sec[1]})
    return info

def main():
    parser = argparse.ArgumentParser(description="This scripts creates a report about the securities in the Fix data.")
    parser.add_argument("-p","--path", help="Path fix data.")    

    args = parser.parse_args()
    path = args.path
    
    fixfile = bz2.BZ2File(path,'rb')
    result = []
    pool = mp.Pool()
    for rmap in pool.imap_unordered(process, fixfile,chunksize=120000):
        result.append(rmap)
    fixfile.close()
    
    wk = report(result)
    wk_report = pd.DataFrame(wk)
    wk_report.to_csv(path[:-3])
    
"""
    fix_files = []
    for (dirpath, dirnames, filenames) in walk(path):
        fix_files.extend(filenames)
               
    for f in fix_files:            
        with gzip.open(path+"/"+f,'rb') as fixfile:
            fixfile = fixfile.readlines()
            pool = mp.Pool(4)
            result = pool.map(process,fixfile)                
            pool.close()
            info = pd.DataFrame(report(result))
            info.to_csv(path+"/"+f[:-3]+".csv")
"""

if __name__ == "__main__":
    main()
