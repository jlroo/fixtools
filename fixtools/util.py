
"""
Created on Fri Jul 22 17:33:13 2016
@author: jlroo
"""

import re
import gzip


"""
               def periods

This function returns the number of trading days in the 
the fix data and its associate number of messages.

returns a dictionaty

{DAY: VOLUME}

"""

def trade_days(path):
    
    periods = []

    if path[-3:] != ".gz":
        fixfile = open(path, "rb")
    else:
        fixfile = gzip.open(path,'rb')
    
    week = {}

    for line in fixfile:
        date = int(re.search(b'(\x0152=)(\d\d\d\d\d\d\d\d)',line).group(2))
        if date not in week.keys():
            week[int(date)] = 1
        else:
            week[int(date)] +=1
    
    for day,cnt in week.items():
        periods.append({'TradeDay':day,'Volume':cnt})
    
    return periods


"""
                    def week_to_day

The week to day function take a path to the fix file
and a list with days corresponding to the trading of
that week and breaks the Fix week file into its 
associate trading days.

This functions creates a new gzip file located in
the same path as the weekly data.
        
"""

def week_to_day(path,dates):
    
    if type(dates) != list:
        raise ValueError("Invalid data type, argument dates must be a list.")
        
    if type(dates[0]) == int or type(dates[0]) == str:
        raise ValueError("Invalid data type, argument dates must be a lsit of bytes variable")

    for day in dates:
        if path[-3:] != ".gz":
            fixfile = open(path, "rb")
        else:
            fixfile = gzip.open(path,'rb')
        path_out = path[:-3]+"_DAY_"+day.decode()+".gz"
        with gzip.open(path_out,'wb') as fixday:
            for line in fixfile:
                if b"\x0175="+day in line:
                    fixday.write(line)
                else:
                    pass
        fixfile.close()


"""
                def group_by

This function takes a path to a fix file and
a security id in order to create a new fix file
with just the messages from that security.                

"""

def group_by(path,security):
    if path[-3:] != ".gz":
        fixfile = open(path, "rb")
    else:
        fixfile = gzip.open(path,'rb')
    data_out = path[:-3]+"_ID"+security+".gz"
    security = b"\x0148="+security.encode()
    with gzip.open(data_out,'wb') as fixsec:
        for line in fixfile:
            if security in line:
                header = line.split(b'\x01279')[0]
                msgtype = re.search(b'(\x0135=)(.*)(\x01)',header).group(2)
                msgtype = msgtype.split(b'\x01')[0]
                if b'X' != msgtype:
                    fixsec.write(line)
                else:
                    body = line.split(b'\x0110=')[0]
                    body = body.split(b'\x01279')[1:]
                    body = [b'\x01279'+ entry for entry in body]
                    end = b'\x0110' + line.split(b'\x0110')[-1]
                    for entry in body:
                        if security in entry:
                            header += entry
                        else:
                            pass
                    msg = header+end
                    fixsec.write(msg)
            else:
                pass
    fixfile.close()

"""
                    Def FixData

This class returns that number a report of the fix data
contracts and volume.

"""

class FixData:
    
    stats = []
    
    def __init__(self,path):
        
        if path[-3:] != ".gz":
            fixfile = open(path, "rb")
        else:
            fixfile = gzip.open(path,'rb')
        contr = {}
    
        for line in fixfile:
                                
            sec = re.search(b'(\x0148\=)(.*)(\x01)',line)
            sec = sec.group(2)
            sec = int(sec.split(b'\x01')[0])
            secdes = re.search(b'(\x01107\=)(.*)(\x01)',line)
            secdes = secdes.group(2)
            secdes = secdes.split(b'\x01')[0].decode()
            secprc = re.search('(^E.*)(\s)(P|C)(\d*)',secdes)
            
            if sec not in contr.keys():
                contr[sec] = {'num':1,'desc':secdes,'type':'','price':0}
                if secprc is not None:
                    if 'C' in secprc.group(0):
                        contr[sec]['type'] = 'C'
                        contr[sec]['price'] = int(secprc.group(4))
                    else:
                        contr[sec]['type'] = 'P'
                        contr[sec]['price'] = int(secprc.group(4))
                else:
                    continue
            else:
                contr[sec]['num']+=1
                        
        fixfile.close()
                       
        for secid,sec in contr.items():            
            self.stats.append({'SecurityID':secid,
                                   'SecurityDesc':sec['desc'],
                                   'Volume':sec['num'],
                                    'Type':sec['type'],
                                    'Price':sec['price']})
                
