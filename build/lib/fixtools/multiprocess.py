#!/opt/anaconda3/bin/python

"""
Created on Mon Aug  8 11:56:15 2016

@author: jlroo
"""
import argparse
import bz2
import os
import re
from collections import Counter
import multiprocessing as mp

def process(line):
    # GET SECURITY ID
    sec = re.search(b'(\x0148\=)(.*)(\x01)',line)
    sec = sec.group(2).split(b'\x01')[0]
    # GET SECURITY DESCRIPTION
    secdes = re.search(b'(\x01107\=)(.*)(\x01)',line)
    secdes = secdes.group(2).split(b'\x01')[0]
    # GET SENDING DATE TAG 52
    day = line[line.find(b'\x0152=')+4:line.find(b'\x0152=')+12]
    return b','.join([sec,secdes,day])+b'\n'

def volumeRpt(data,filename="",fileOut=True):
    volume = Counter(entry.split(b',')[0] for entry in data)
    secinfo = dict((entry.split(b',')[0],
              {"desc":entry.split(b',')[1],
              "sday":entry.split(b',')[2][0:8]}) for entry in data)
    header = b'SecurityID,SecurityDesc,SendingDate,Volume'+b'\n'
    if fileOut==True:
        with open(filename+".csv","wb") as file:
            file.write(header)
            for sec in volume.items():
                file.write(b','.join([sec[0],secinfo[sec[0]]['desc'],secinfo[sec[0]]['sday'],str(sec[1]).encode()])+b'\n')
    else:
        vol = []
        vol.append(header)
        for sec in volume.items():
                vol.append(b','.join([sec[0],secinfo[sec[0]]['desc'],secinfo[sec[0]]['sday'],str(sec[1]).encode()])+b'\n')
        return vol

def main():
    parser = argparse.ArgumentParser(description="This scripts creates a report about the securities in the Fix data.")
    parser.add_argument("-p","--path", help="Path to fix data.")  
    args = parser.parse_args()
    PATH = args.path
    path, names, file = next(os.walk(PATH))
    for f in file:
        with bz2.open(path+f,'rb') as fixfile:
            pool = mp.Pool()
            if os.path.getsize(path+f)<2.00*10**9:
                proc = pool.map(process,fixfile)
                volumeRpt(proc,path+f[:-4],fileOut=True)
            else:
                vol = []
                for chunck in pool.imap(process,fixfile):
                    vol.append(volumeRpt(chunck,fileOut=False))
                with open(path+f[:-4]+".csv","wb") as file:
                    file.writelines(vol)
            pool.close()

if __name__ == "__main__":
    main()
