
"""
Created on Fri Jul 22 17:33:13 2016
@author: jlroo
"""

from collections import Counter
import gzip
import bz2


"""
                    Def FixData

This class returns that number a report of the fix data
contracts and volume.

"""

def openFix(path,period="weekly",compression=True):
    if period != "weekly" or period != "daily":
        raise ValueError("Supported time period: weekly or daily")
    src = {"path":path,"period":period}
    if compression == False:
        fixfile = open(path,'rb')
    else:
        if path[-3:] == ".gz":
            fixfile = gzip.open(path,'rb')
        elif path[-4:] == ".bz2":
            fixfile = bz2.BZ2File(path,'rb')
        else:
            raise ValueError("Supported compressions gzip and bz2. \
                              For uncompressed files change compression \
                              flag to False.")
    return FixData(fixfile,src)

class FixData:
    dates = []
          
    def __init__(self,fixfile,src):
        self.data = fixfile
        self.path = src["path"]
        line0 = self.data.peek().split(b"\n")[0]
        d0 = line0[line0.find(b'\x0152=')+4:line0.find(b'\x0152=')+12]
        if src["period"] == "weekly":
            self.dates = list(range(int(d0.decode()),int(d0.decode())+6))
        elif src["period"] == "daily":
            self.dates = list(range(int(d0.decode()),int(d0.decode())+2))
        else:
            raise ValueError("Supported time period: weekly or daily")         
            

    """
                       def tradeDays
        
        This function returns the number of trading days in the 
        the fix data and the number of messages per day.
        
        returns a dictionary
        
        {DAY: VOLUME}
    """
             
    def volume(self):
        tday = lambda line: line[line.find(b'\x0152=')+4:line.find(b'\x0152=')+12]        
        vol = Counter(map(tday,self.data))
        self.data.seek(0)
        return vol


    """
                        def splitBy
        
        The week to day function take a path to the fix file
        and a list with days corresponding to the trading of
        that week and breaks the Fix week file into its 
        associate trading days.
        
        This functions creates a new gzip file located in
        the same path as the weekly data.
        
    """

    def splitBy(self, dates,compression="bzip"):
        
        if type(dates) != list:
            raise ValueError("Expected a list of dates.")
        if compression == "bzip":
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
            self.data.seek(0)
        elif compression == "gzip":
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
            self.data.seek(0)
        else:
            raise ValueError("Invalid compression type.")


    """
                        def filterBy
        
        This function takes a path to a fix file and
        a security id in order to create a new fix file
        with just the messages from that security.                
    
    """

    def filterBy(self,securityID,compression="bzip"): 
        if compression == "bzip":
            print("Using compression: bzip")
            path_out = self.path[:-4]+"_ID"+str(securityID)+".bz2"
            secID = "\x0148="+securityID+"\x01"
            secID = secID.encode()
            tag = lambda line: True if secID in line else False
            with bz2.open(path_out,'w') as fixsec:                
                for line in iter(filter(tag,self.data)):
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
                        fixsec.write(msg)
                    else:
                        fixsec.write(line)
            self.data.seek(0)