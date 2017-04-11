
"""
Created on Fri Jul 22 17:33:13 2016
@author: jlroo
"""

import gzip
import bz2
import multiprocessing as mp
import re
import datetime
from collections import defaultdict

"""
                    Def FixData

This class returns that number a report of the fix data
contracts and volume.

"""

def openFix(path,period="weekly",compression=True):
    if period.lower() not in ("weekly","daily","monthly"):
        raise ValueError("Supported time period: weekly or daily")
    src = {"path":path,"period":period.lower()}
    if compression == False:
        if path[-4:].lower in (".zip",".tar"):
            raise ValueError("Supported compressions gzip, bz2 or bytes data")
        else:
            fixfile = open(path,'rb')
    else:
        if path[-3:] == ".gz":
            fixfile = gzip.open(path,'rb')
        elif path[-4:] == ".bz2":
            fixfile = bz2.BZ2File(path,'rb')
        else:
            raise ValueError("Supported files gzip,bz2, uncompress bytes file. \
                              For uncompressed files change compression flag to False.")
    return FixData(fixfile,src)

def settlementDay(date,weekNumber,dayOfWeek):
    weekday = {'monday':0,'tuesday':1,'wednesday':2,
               'thursday':3,'friday':4,'saturday':5,
               'sunday':6}
    date = datetime.datetime(date.year,date.month,date.day)
    if date.weekday() == weekday[dayOfWeek.lower()]:
        if date.day // 7 == (weekNumber - 1):
            return True
    return False

def contractCode(month,codes="F,G,H,J,K,M,N,Q,U,V,X,Z,F,G,H,J,K,M,N,Q,U,V,X,Z"):
    monthCodes = { k[0]:k[1] for k in enumerate(codes.rsplit(","),1)}
    codesHash = {}
    for index in monthCodes:
        if index%3==0:
            codesHash[index]=(monthCodes[index],{index-2:monthCodes[index-2],index-1:monthCodes[index-1]})
    if month%3==0:
        return codesHash[month][0]
    if month%3==1:
        return codesHash[month+2][1][month]
    if month%3==2:
        return codesHash[month+1][1][month]

def mostLiquid(dates,instrument="",product=""):
    date = datetime.datetime(year=dates[0].year, month=dates[0].month, day=dates[0].day)
    contractYear = lambda yr: yr[-1:] if yr[1:3] != "00" else yr[-1:]
    expWeek = next(filter(lambda day: settlementDay(day,3,'friday'),dates),None)
    expired = True if date.day>16 else False
    secCode = contractCode(date.month)
    if expWeek != None or expired:
        if product.lower() in ("fut","futures"):
                if date.month%3==0:
                    secCode = contractCode(date.month+3)
                if date.month%3==1:
                    secCode = contractCode(date.month+2)
                if date.month%3==2:
                    secCode = contractCode(date.month+1)
        if product.lower() in ("opt","options"):
            secCode = contractCode(date.month+1)
    secDesc = instrument + secCode + contractYear(str(date.year))
    return secDesc

fixDate = ""

def __dayFilter__(line):
    global fixDate
    filterDate = b'\x0152='+str(fixDate).encode()
    if filterDate in line:
        return line

def __metrics__(line):
    # GET SECURITY ID
    sec = re.search(b'(\x0148\=)(.*)(\x01)',line)
    sec = sec.group(2).split(b'\x01')[0]
    # GET SECURITY DESCRIPTION
    secdes = re.search(b'(\x01107\=)(.*)(\x01)',line)
    secdes = secdes.group(2).split(b'\x01')[0]
    # GET SENDING DATE TAG 52
    day = line.split(b'\x0152=')[1].split(b'\x01')[0][0:8]
    return b','.join([sec,secdes,day])


class FixData:
    dates = []
    stats = {}

    def __init__(self,fixfile,src):
        self.data = fixfile
        self.path = src["path"]

        peek = self.data.peek().split(b"\n")[0]
        day0 = peek[peek.find(b'\x0152=')+4:peek.find(b'\x0152=')+12]

        if src["period"] == "weekly":
            start = datetime.datetime(  year = int(day0[:4]),
                                        month = int(day0[4:6]),
                                        day = int(day0[6:8]))
            self.dates = [start + datetime.timedelta(days=i) for i in range(6)]
        else:
            raise ValueError("Supported time period: weekly data to get dates")


    """
                       def securities

        This function returns the securities in the data
        by the expiration month

        returns a dictionary

        {MONTH: {SEC_ID:SEC_DESC}

    """

    def securities(self):
        contracts = {k:{"FUT":{},"OPT":{},"SPREAD":{}} for k in "F,G,H,J,K,M,N,Q,U,V,X,Z".split(",")}
        for line in self.data:
            desc = line[line.find(b'd\x01'):line.find(b'd\x01')+1]
            if desc != b'd' : break
            secID = line.split(b'\x0148=')[1].split(b'\x01')[0]
            secDesc = line.split(b'\x01107=')[1].split(b'\x01')[0]
            for month in contracts.keys():
                if month.encode() in secDesc:
                    if len(secDesc)< 7:
                        contracts[month]['FUT'][int(secID)] = secDesc.decode()
                    if b'P' in secDesc or b'C' in secDesc:
                        contracts[month]['OPT'][int(secID)] = secDesc.decode()
                    if b'-' in secDesc:
                        contracts[month]['SPREAD'][int(secID)] = secDesc.decode()
        self.data.seek(0)
        return contracts


    """
                       def dataMetrics

        This function returns the number of messages
        sent in a particular date.

        returns a dictionary

        {DAY: VOLUME}
    """

    def dataMetrics(self,chunksize=10**4,fileOut=False,path=""):
        desc = {}
        table = defaultdict(dict)
        with mp.Pool() as pool:
            dataMap = pool.imap(__metrics__,self.data,chunksize)
            for entry in dataMap:
                day = entry.split(b',')[2][0:8].decode()
                sec = entry.split(b',')[0].decode()
                secdesc = entry.split(b',')[1].decode()
                desc[sec] = secdesc
                if sec not in table[day].keys():
                    table[day][sec]=1
                else:
                    table[day][sec]+=1
        if fileOut==False:
            fixStats = defaultdict(dict)
            for day in sorted(table.keys()):
                fixStats[day]=defaultdict(dict)
                for sec in table[day]:
                    fixStats[day][sec]["desc"] = desc[sec]
                    fixStats[day][sec]["vol"] = table[day][sec]
            return fixStats
        else:
            header = b'SecurityID,SecurityDesc,Volume,SendingDate'+b'\n'
            for day in sorted(table.keys()):
                with open(path+"stats_"+day+".csv","wb") as f:
                    f.write(header)
                    for sec in table[day]:
                        f.write(b','.join([ sec.encode(),
                                            desc[sec].encode(),
                                            str(table[day][sec]).encode(),
                                            day.encode()]) + b'\n')
        self.data.seek(0)


    """
                        def splitBy

        The week to day function take a path to the fix file
        and a list with days corresponding to the trading of
        that week and breaks the Fix week file into its
        associate trading days.

        This functions creates a new gzip file located in
        the same path as the weekly data.

    """

    def splitBy(self,dates,chunksize=10**4,fileOut=False):
        for day in dates:
            global fixDate
            fixDate = str(day).encode()
            path_out = self.path[:-4]+"_"+str(day)+".bz2"
            with mp.Pool() as pool:
                    msgDay = pool.imap(__dayFilter__,self.data,chunksize)
                    if fileOut==True:
                        with bz2.open(path_out,'ab') as f:
                            for entry in msgDay:
                                f.write(entry)
                    else:
                        for entry in msgDay:
                            return msgDay
            self.data.seek(0)

    """
                        def filterBy

        This function takes a path to a fix file and
        a security id in order to create a new list or
        fix file with messages from that security.

    """

    def filterBy(self,securityID,fileOut=False):
        secID = b"\x0148="+securityID.encode()+b"\x01"
        tag = lambda line: True if secID in line else False
        if fileOut==False:
            sec = []
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
                    sec.append(msg)
                else:
                    sec.append(line)
            return sec
        else:
            print("Using default compression bzip")
            path_out = self.path[:-4]+"_ID"+str(securityID)+".bz2"
            with bz2.open(path_out,'wb') as fixsec:
                filtered = self.filterBy(securityID,fileOut=False)
                fixsec.writelines(filtered)
        self.data.seek(0)