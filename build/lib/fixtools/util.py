
"""
Created on Fri Jul 22 17:33:13 2016
@author: jlroo
"""

from collections import Counter
"""
               def tradeDays

This function returns the number of trading days in the 
the fix data and the number of messages per day.

returns a dictionary

{DAY: VOLUME}


def tradeDays(fixfile):
      
    week = {}
    tday = lambda line: line[line.find(b'\x0152=')+4:line.find(b'\x0152=')+12]        
    days = list(map(tday,fixfile))
    week = Counter(days)
    
    periods = {}
    for day,cnt in week.items():
        periods[int(day.decode())]=cnt

    fixfile.seek(0)
    return periods

"""
"""

                    def week_to_day

The week to day function take a path to the fix file
and a list with days corresponding to the trading of
that week and breaks the Fix week file into its 
associate trading days.

This functions creates a new gzip file located in
the same path as the weekly data.
        


def splitBy(fixfile,dates,compression="bzip"):
    
    if type(dates) != list:
        raise ValueError("Expected a list of dates.")

    if compression == "bzip":
        import bz2
        print("Using compression: bzip")
        for day in dates:
            path_out = path[:-3]+"_DAY_"+str(day)+".bz2"
            date = b"\x0152="+str(day).encode()
            with bz2.open(path_out,'w') as fixday:
                for line in fixfile:
                    if date in line:
                        fixday.write(line)
                    else:
                        break
    elif compression == "gzip":
        import gzip
        print("Using compression: gzip")
        for day in dates:
            date = b"\x0152="+str(day).encode()
            path_out = path[:-3]+"_DAY_"+str(day)+".gz"
            with gzip.open(path_out,'wb') as fixday:
                for line in fixfile:
                    if date in line:
                        fixday.write(line)
                    else:
                        break
    else:
        raise ValueError("Invalid compression type.")

"""
"""
                def group_by

This function takes a path to a fix file and
a security id in order to create a new fix file
with just the messages from that security.                



def group_by(path,security):
    if path[-3:] != ".gz":
        fixfile = open(path, "rb")
    else:
        import gzip
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
"""
                    Def FixData

This class returns that number a report of the fix data
contracts and volume.


class FixData:
       
    def __init__(self,path):
        
        if path[-3:] != ".gz":
            fixfile = open(path, "rb")
        else:
            import gzip
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
                

"""

def openFix(path,compression=True):
    if compression == False:
        fixfile = open(path,'rb')
    else:
        if path[-3:] == ".gz":
            import gzip
            fixfile = gzip.open(path,'rb')
        elif path[-4:] == ".bz2":
            import bz2
            fixfile = bz2.BZ2File(path,'rb')
        else:
            raise ValueError("Supported compressions gzip and bz2. \
                              For uncompressed files change compression \
                              flag to False.")
    return FixData(fixfile,path)

class FixData:
       
    def __init__(self,fixfile,path):
        self.data = fixfile
        self.path = path
             
    def tradeReport(self):
        from collections import namedtuple
        week = {}
        tday = lambda line: line[line.find(b'\x0152=')+4:line.find(b'\x0152=')+12]        
        days = list(map(tday,self.data))
        week = Counter(days)
        stats = {}
        for day,cnt in week.items():
            stats[int(day.decode())]=cnt
        dates = list(stats.keys())
        self.data.seek(0)
        report = namedtuple('report',['dates','stats'])
        return report(dates,stats)
        
    def tradeDays(self):
        tday = lambda line: line[line.find(b'\x0152=')+4:line.find(b'\x0152=')+12]        
        days = set(map(tday,self.data))
        self.data.seek(0)
        return days

    def splitBy(self, dates,compression="bzip"):
        
        if type(dates) != list:
            raise ValueError("Expected a list of dates.")
    
        if compression == "bzip":
            import bz2
            print("Using compression: bzip")
            for day in dates:
                path_out = self.path[:-4]+"_DAY_"+str(day)+".bz2"
                date = b"\x0152="+str(day).encode()
                with bz2.open(path_out,'w') as fixday:
                    for line in self.data:
                        if date in line:
                            fixday.write(line)
                        else:
                            break
        elif compression == "gzip":
            import gzip
            print("Using compression: gzip")
            for day in dates:
                date = b"\x0152="+str(day).encode()
                path_out = self.path[:-3]+"_DAY_"+str(day)+".gz"
                with gzip.open(path_out,'wb') as fixday:
                    for line in self.data:
                        if date in line:
                            fixday.write(line)
                        else:
                            break
        else:
            raise ValueError("Invalid compression type.")
            
    def groupBy(self,securityID,compression="bzip"): 
        if compression == "bzip":
            import bz2
            print("Using compression: bzip")
            path_out = self.path[:-4]+"_ID"+str(securityID)+".bz2"
            secID = b"\x0148="+str(securityID).encode()
            try:
                with bz2.open(path_out,'w') as fixsec:
                    while True:
                        tag = lambda line: True if secID in line else False
                        line = next(filter(tag,self.data))
                        secID = b'\x0148='+str(securityID).encode()+b'\x01'
                        header = line.split(b'\x01279')[0]
                        msgtype = line[line.find(b'35=')+3:line.find(b'35=')+4]    
                        if b'X' == msgtype:
                            body = line.split(b'\x0110=')[0]
                            body = body.split(b'\x01279')[1:]
                            body = [b'\x01279'+ entry for entry in body]
                            end = b'\x0110' + line.split(b'\x0110')[-1]
                            
                            for entry in body:
                                if secID in entry:
                                    header += entry
                                else:
                                    pass
                            
                            msg = header+end
                            return fixsec.write(msg)
                        else:
                            return fixsec.write(line)
            except StopIteration:
                pass
            self.data.seek(0)