
"""
Created on Fri Jul 22 17:33:13 2016
@author: jlroo
"""

import re as __re
import gzip as __gzip

############################################
#               def trade day
############################################

def periods(filename):    
    d = __re.search("(\d.*)(\D)(.*\d)",filename)
    if d is None:
        raise ValueError("Invalid filename format, expected format: YYYYMMDD_YYYYMMDD" )
    periods = list(range(int(d.group(1)),int(d.group(3))+1,1))
    periods = [str(d).encode() for d in periods]
    return periods

############################################
#               def read fix
############################################

def read_fix(filename):
    if filename[-3:] != ".gz":
        f = open(filename, "rb")
    else:
        f = __gzip.open(filename,'rb')
    return f

############################################
#               def write out
############################################

def to_day(path,periods):
    for day in periods:
        if day is not bytes:
            day = day.encode()
        fixfile = read_fix(path)
        path_out = day.decode()+".gz"
        with __gzip.open(path_out,'wb') as fixday:
            for line in fixfile:
                if b"\x0175="+day in line:
                    fixday.write(line)
                else:
                    pass
        fixfile.close()

############################################
#               def security
############################################

def group_by(fixfile,sec):
    data_out = "ID"+sec+".gz"
    sec = sec.encode()
    with __gzip.open(data_out,'wb') as fixsec:
        for line in fixfile:
            if b"\x0148="+sec in line:
                header = line.split(b'\x01279')[0]
                msgtype = __re.search(b'(\x0135=)(.*)(\x01)',header).group(2)
                msgtype = msgtype.split(b'\x01')[0]
                if b'X' != msgtype:
                    fixsec.write(line)
                else:
                    body = line.split(b'\x0110=')[0]
                    body = body.split(b'\x01279')[1:]
                    body = [b'\x01279'+ entry for entry in body]
                    end = b'\x0110' + line.split(b'\x0110')[-1]
                    for entry in body:
                        if b'\x0148='+sec in entry:
                            header += entry
                        else:
                            pass
                    msg = header+end
                    fixsec.write(msg)
            else:
                pass
    fixfile.close()

#####################################################################
#                       NUMBER OF CONTRACTS                    
#####################################################################

class contracts:
    
    report = []
    
    def __init__(self,fixfile):
        contr = {}
        for line in fixfile:
            sec = __re.search(b'(\x0148\=)(.*)(\x01)',line)
            sec = sec.group(2)
            sec = int(sec.split(b'\x01')[0])
            secdes = __re.search(b'(\x01107\=)(.*)(\x01)',line)
            secdes = secdes.group(2)
            secdes = secdes.split(b'\x01')[0].decode()
            secprc = __re.search('(^E.*)(\s)(P|C)(\d*)',secdes)
            
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
            self.report.append({'SecurityID':secid,
                                   'SecurityDesc':sec['desc'],
                                   'Volume':sec['num'],
                                    'Type':sec['type'],
                                    'Price':sec['price']})
                