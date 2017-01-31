
"""
Created on Fri Jul 22 17:33:13 2016
@author: jlroo
"""

from collections import Counter
import gzip
import bz2
import multiprocessing as mp
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
            raise ValueError("Supported compressions gzip and bz2. \
                              For uncompressed files change compression \
                              flag to False.")
    return FixData(fixfile,src)

SecurityDescription = ""

def __secFilter__(line):
    global SecurityDescription
    secDesc = b'\x01107='+SecurityDescription.encode()+b'\x01'
    mkRefresh = line[line.find(b'\x0135=')+4:line.find(b'\x0135=')+5] in b'\x0135=X\x01'
    if mkRefresh and secDesc in line:
        return line

fixDate = ""

def __dayFilter__(line):
    global fixDate
    filterDate = b'\x0152='+str(fixDate).encode()
    if filterDate in line:
        return line
        
def __tradeDay__(line):
    return line.split(b'\x0175=')[1].split(b'\x01')[0]

class FixData:
    dates = []
    volume = {}
    book = b''
    BookSeqNum = None
          
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
                       def msgVolume
        
        This function returns the number of messages
        sent in a particular date.
        
        returns a dictionary ( counter type )
        
        {DAY: VOLUME}
    """
             
    def msgVolume(self):
        with mp.Pool() as pool:
            dates = pool.map(__tradeDay__,self.data)
            self.volume = Counter(entry.split(b',')[0] for entry in dates)
        self.data.seek(0)
        return self.volume 

    """
                        def splitBy
        
        The week to day function take a path to the fix file
        and a list with days corresponding to the trading of
        that week and breaks the Fix week file into its 
        associate trading days.
        
        This functions creates a new gzip file located in
        the same path as the weekly data.
        
    """

    def splitBy(self,dates,fileOut=False):
        for day in dates:
            global fixDate
            fixDate = str(day).encode()
            path_out = self.path[:-4]+"_"+str(day)+".bz2"
            with mp.Pool() as pool:
                    msgDay = pool.map(__dayFilter__,self.data)                                 
            if fileOut==True:                    
                with bz2.open(path_out,'w') as f:
                    for entry in msgDay:
                        f.write(entry)
            else:
                self.data.seek(0)
                return msgDay

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

    def initBook(self,SecurityDesc):
        global SecurityDescription
        SecurityDescription = SecurityDesc
        TradeType = lambda line: line[line.find(b'\x01269=')+5:line.find(b'\x01269=')+6]
        EntryType = lambda line: True if b'\x0135=X\x01' in line and TradeType(line) in b'0|1'else None
        secDesc = b'\x01107='+SecurityDescription.encode()+b'\x01'
        book0 = next(filter(EntryType,self.data), None)
        header = book0.split(b'\x01279')[0]
        end = b'\x0110' + book0.split(b'\x0110')[-1]
        body = book0.split(b'\x0110=')[0]
        body = body.split(b'\x01279')[1:]
        body = [b'\x01279'+ entry for entry in body]
        header += b''.join([e if secDesc in e else b'' for e in body])
        self.book = header+end
        self.data.seek(0)
        return self.book

    def updateBook(self,book_body,msg_body):
        bids,offers = book_body[0:10],book_body[10:]
        for entry in msg_body:
            priceLevel = int(entry.split(b'\x011023=')[1])
            entryType = int(entry[entry.find(b'\x01269=')+5:entry.find(b'\x01269=')+6])
            actionType = int(entry[entry.find(b'\x01279=')+5:entry.find(b'\x01279=')+6])
            if entryType == 0: # BID tag 269=0
                if actionType == 1: # CHANGE 279=1
                    bids[priceLevel-1] = entry
                elif actionType == 0: # NEW tag 279=0
                    if priceLevel == 10:
                        bids[9] = entry
                    else:
                        bids.insert(priceLevel-1,entry)
                        for i in range(priceLevel,10):
                            bids[i] = bids[i].replace(b'\x011023='+str(i).encode(),b'\x011023='+str(i+1).encode())
                        bids.pop()
                else:  # b'\x01279=2' DELETE
                    delete = entry.split(b'\x011023=')[0]+b'\x011023=10'
                    if priceLevel == 10:
                        bids[9] = delete
                    else:
                        bids.pop(priceLevel-1)
                        for i in range(priceLevel,10):
                            bids[i-1] = bids[i-1].replace(b'\x011023='+str(i+1).encode(),b'\x011023='+str(i).encode())
                        bids.append(delete)
            else: # OFFER tag 269=1
                if actionType == 1: # CHANGE 279=1
                    offers[priceLevel-1] = entry
                elif actionType == 0: # NEW tag 279=0
                    if priceLevel == 10:
                        offers[9] = entry
                    else:
                        offers.insert(priceLevel-1,entry)
                        for i in range(priceLevel,10):
                            offers[i] = offers[i].replace(b'\x011023='+str(i).encode(),b'\x011023='+str(i+1).encode())
                        offers.pop()
                else:  # b'\x01279=2' DELETE
                    delete = entry.split(b'\x011023=')[0]+b'\x011023=10'
                    if priceLevel == 10:
                        offers[9] = delete
                    else:
                        offers.pop(priceLevel-1)
                        for i in range(priceLevel,10):
                            offers[i-1] = offers[i-1].replace(b'\x011023='+str(i+1).encode(),b'\x011023='+str(i).encode())
                        offers.append(delete)
        return bids,offers

    def buildbook(self,SecurityDesc):
        global SecurityDescription
        SecurityDescription = SecurityDesc
        if self.book==b'' or self.BookSeqNum==None:
            self.book = self.initBook(SecurityDescription)
            BookSeqNum = int(self.book.split(b'\x0134=')[1].split(b'\x01')[0])
        secDesc = b'\x01107='+SecurityDescription.encode()+b'\x01'
        tradeType = lambda line: line[line.find(b'\x01269=')+5:line.find(b'\x01269=')+6] in b'0|1'     
        with mp.Pool() as pool:
            msgMap = pool.map(__secFilter__,self.data)
        MsgSeqNum = lambda i:int(i.split(b'\x0134=')[1].split(b'\x01')[0])
        updates = lambda e: e is not None and MsgSeqNum(e)>BookSeqNum
        messages = iter(filter(updates,msgMap))
        for msg in messages:
        ########################## PRIVOUS BOOK #############################
            book_body = self.book.split(b'\x0110=')[0]
            book_body = book_body.split(b'\x01279')[1:]
            book_body = [b'\x01279'+ entry for entry in book_body]
            #bids,offers = book_body[0:10],book_body[10:]
        #####################################################################
            book_header = msg.split(b'\x01279')[0]
            book_end = b'\x0110' + msg.split(b'\x0110')[-1]
            msg_body = msg.split(b'\x0110=')[0].split(b'\x01279')[1:]
            msg_body = [b'\x01279'+ e if secDesc in e and b'\x01276' not in e else None for e in msg_body]
            msg_body = iter(filter(lambda e: e is not None and tradeType(e),msg_body))
        ############################ BOOK UPDATE  ###########################
            bids,offers=self.updateBook(book_body,msg_body)
            book_body = bids+offers
            book_header += b''.join([e for e in book_body])
            self.book = book_header + book_end
            return self.book
        self.data.seek(0)