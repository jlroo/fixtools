
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
    days = {'monday':0,'tuesday':1,'wednesday':2,
            'thursday':3,'friday':4,'saturday':5,
            'sunday':6}
    dates = datetime.datetime(date.year,date.month,date.day)
    if dates.weekday() == days[dayOfWeek.lower()]:
        if dates.day // 7 == (weekNumber - 1):
            return True
    return False

def mostLiquid(wk):
    date = datetime.datetime(year=wk[0].year, month=wk[0].month, day=wk[0].day)
    contractID = lambda yr: yr[-1:] if yr[1:3] != "00" else yr[-1:]
    expWeek = next(filter(lambda day: settlementDay(day,3,'friday'),wk),None)
    expired = True if date.month in (3,6,9,12) and date.day>16 else False
    if date.month <= 3:
        secDesc = "ESH" + contractID(str(date.year))
        if expWeek != None or expired:
            secDesc = secDesc.replace("H","M")
    elif date.month >= 4 and date.month < 6:
        secDesc = "ESM" + contractID(str(date.year))
        if expWeek != None or expired:
            secDesc = secDesc.replace("M","U")
    elif date.month >= 6 and date.month < 9:
        secDesc = "ESU" + contractID(str(date.year))
        if expWeek != None or expired:
            secDesc = secDesc.replace("U","Z")
    elif date.month >= 9:
        secDesc = "ESZ" + contractID(str(date.year))
        if expWeek != None or expired:
            secDesc = secDesc.replace("Z","H")
    return secDesc

SecurityDescription = ""

def __secFilter__(line):
    global SecurityDescription
    secDesc = b'107='+SecurityDescription.encode()+b'\x01' in line
    mkRefresh = b'35=X\x01' in line
    if mkRefresh and secDesc:
        return line

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
    books,dates = [],[]
    stats = {}
    book = b''
    securityDesc = ""

    def __init__(self,fixfile,src):
        self.data = fixfile
        self.path = src["path"]
        line0 = self.data.peek().split(b"\n")[0]
        d0 = line0[line0.find(b'\x0152=')+4:line0.find(b'\x0152=')+12]

        if src["period"] == "weekly":
            start = datetime.datetime(  year = int(d0[:4]),
                                        month = int(d0[4:6]),
                                        day = int(d0[6:8]))
            self.dates = [start + datetime.timedelta(days=i) for i in range(6)]
        else:
            raise ValueError("Supported time period: weekly data to get dates")

        self.securityDesc = mostLiquid(self.dates)

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

    """
                        def initBook

        This function finds the first message with
        bids & offers (opening book) and returns it as
        the initial order book for that trading session

    """

    def initBook(self,securityDesc):
        global SecurityDescription
        SecurityDescription = securityDesc
        secDesc = b'\x01107='+SecurityDescription.encode()+b'\x01'
        msgType = lambda line: b'35=X\x01' in line and secDesc in line
        tradeType = lambda line: line[line.find(b'\x01269=')+5:line.find(b'\x01269=')+6] in b'0|1'
        msgOpen = lambda line: True if msgType(line) and tradeType(line) else None
        book0 = next(filter(msgOpen,self.data), None)
        header = book0.split(b'\x01279=')[0]
        end = b'\x0110' + book0.split(b'\x0110')[-1]
        body = book0.split(b'\x0110=')[0]
        body = body.split(b'\x01279')[1:]
        body = [b'\x01279'+ entry for entry in body]
        header += b''.join([e if secDesc in e else b'' for e in body])
        self.book = header+end
        self.data.seek(0)
        return self.book


    def buildbook(self,securityDesc,chunksize=10**4):
        global SecurityDescription
        SecurityDescription = securityDesc
        secDesc = b'\x01107='+SecurityDescription.encode()+b'\x01'
        tradeType = lambda line: line[line.find(b'\x01269=')+5:line.find(b'\x01269=')+6] in b'0|1'
        MsgSeqNum = lambda line:int(line.split(b'\x0134=')[1].split(b'\x01')[0])
        self.book = self.initBook(securityDesc)
        bookSeqNum = int(self.book.split(b'\x0134=')[1].split(b'\x01')[0])
        updates = lambda entry: entry is not None and MsgSeqNum(entry)>bookSeqNum
        with mp.Pool() as pool:
            msgMap = pool.imap(__secFilter__,self.data,chunksize)
            messages = iter(filter(updates,msgMap))
            for msg in messages:
            ########################## PRIVIOUS BOOK #############################
                book_body = self.book.split(b'\x0110=')[0]
                book_body = book_body.split(b'\x01279')[1:]
                book_body = [b'\x01279'+ entry for entry in book_body]
            #####################################################################
                book_header = msg.split(b'\x01279')[0]
                book_end = b'\x0110' + msg.split(b'\x0110')[-1]
                msg_body = msg.split(b'\x0110=')[0].split(b'\x01279')[1:]
                msg_body = [b'\x01279'+ e if secDesc in e and b'\x01276' not in e else None for e in msg_body]
                msg_body = iter(filter(lambda e: e is not None and tradeType(e),msg_body))
            ############################ BOOK UPDATE  ###########################
                bids,offers = self.updateBook(book_body,msg_body)
                book_body = bids+offers
                book_header += b''.join([e for e in book_body])
                self.book= book_header + book_end
                yield self.book
        self.data.seek(0)


    def updateBook(self,book_body,msg_body):
        topOrder = len(book_body)//2
        bids,offers = book_body[0:topOrder],book_body[topOrder:]
        for entry in msg_body:
            priceLevel = int(entry.split(b'\x011023=')[1])
            entryType = int(entry[entry.find(b'\x01269=')+5:entry.find(b'\x01269=')+6])
            actionType = int(entry[entry.find(b'\x01279=')+5:entry.find(b'\x01279=')+6])
            if entryType == 0: # BID tag 269= esh9[1]
                if actionType == 1: # CHANGE 279=1
                    bids[priceLevel-1] = entry
                elif actionType == 0: # NEW tag 279=0
                    if priceLevel == topOrder:
                        bids[topOrder-1] = entry
                    else:
                        bids.insert(priceLevel-1,entry)
                        for i in range(priceLevel,topOrder):
                            bids[i] = bids[i].replace(b'\x011023='+str(i).encode(),b'\x011023='+str(i+1).encode())
                        bids.pop()
                else:  # b'\x01279=2' DELETE
                    delete = entry.split(b'\x011023=')[0]+b'\x011023=10'
                    if priceLevel == topOrder:
                        bids[topOrder-1] = delete
                    else:
                        bids.pop(priceLevel-1)
                        for i in range(priceLevel,topOrder):
                            bids[i-1] = bids[i-1].replace(b'\x011023='+str(i+1).encode(),b'\x011023='+str(i).encode())
                        bids.append(delete)
            else: # OFFER tag 269=1
                if actionType == 1: # CHANGE 279=1
                    offers[priceLevel-1] = entry
                elif actionType == 0: # NEW tag 279=0
                    if priceLevel == topOrder:
                        offers[topOrder-1] = entry
                    else:
                        offers.insert(priceLevel-1,entry)
                        for i in range(priceLevel,topOrder):
                            offers[i] = offers[i].replace(b'\x011023='+str(i).encode(),b'\x011023='+str(i+1).encode())
                        offers.pop()
                else:  # b'\x01279=2' DELETE
                    delete = entry.split(b'\x011023=')[0]+b'\x011023=10'
                    if priceLevel == topOrder:
                        offers[topOrder-1] = delete
                    else:
                        offers.pop(priceLevel-1)
                        for i in range(priceLevel,topOrder):
                            offers[i-1] = offers[i-1].replace(b'\x011023='+str(i+1).encode(),b'\x011023='+str(i).encode())
                        offers.append(delete)
        return bids,offers