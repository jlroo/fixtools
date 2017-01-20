
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
    if period.lower() not in ("weekly","daily","monthly"):
        raise ValueError("Supported time period: weekly or daily")
    src = {"path":path,"period":period.lower()}
    if compression == False:
        if path[-4:].lower in (".bz2",".zip",".tar"):
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

class FixData:
    dates = []
    volume = {}
    book0 = b''
          
    def __init__(self,fixfile,src):
        self.data = fixfile
        self.path = src["path"]
        line0 = self.data.peek().split(b"\n")[0]
        d0 = line0[line0.find(b'\x0152=')+4:line0.find(b'\x0152=')+12]
        if src["period"] == "weekly":
            self.dates = list(range(int(d0.decode()),int(d0.decode())+6))
        elif src["period"] == "daily":
            self.dates = list(range(int(d0.decode()),int(d0.decode())+2))
        elif src["period"] == "monthly":
            raise ValueError("Monthly needs to be implemented")         
            #self.dates = list(range(int(d0.decode()),int(d0.decode())+2))
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
        import multiprocessing as mp
        tday = lambda line: line[line.find(b'\x0152=')+4:line.find(b'\x0152=')+12]        
        with mp.Pool() as pool:                
            dates = pool.map(tday,self.data)
            vol = Counter(entry.split(b',')[0] for entry in dates)
        self.data.seek(0)
        self.volume = vol
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

    def initBook(self,SecDescription):
        TradeType = lambda line: line[line.find(b'\x01269=')+5:line.find(b'\x01269=')+6]
        EntryType = lambda line: True if b'\x0135=X\x01' in line and TradeType(line) in b'0|1'else None
        secDesc = b'\x01107='+SecDescription.encode()+b'\x01'
        book0 = next(filter(EntryType,self.data), None)
        header = book0.split(b'\x01279')[0]
        end = b'\x0110' + book0.split(b'\x0110')[-1]
        body = book0.split(b'\x0110=')[0]
        body = body.split(b'\x01279')[1:]
        body = [b'\x01279'+ entry for entry in body]
        header += b''.join([e if secDesc in e else b'' for e in body])
        self.book0 = header+end
        self.data.seek(0)
        return self.book0
    
    def buildbook(self,SecDescription,fileOut=True):
        if self.book0==b'':
            book0 = self.initBook(SecDescription)
        else:
            book0 = self.book0
        tradeType = lambda line: line[line.find(b'\x01269=')+5:line.find(b'\x01269=')+6] # BID or OFFER
        msgType = lambda line: True if b'\x0135=X\x01' in line and tradeType(line) in b'0|1'else None # UPDATE Msg
        secDesc = b'\x01107='+SecDescription.encode()+b'\x01'
        books = []
        books.append(book0)
        if fileOut==False:
            messages = iter(filter(msgType,self.data))
            next(messages) # Start from second msg first is the initial book
            for msg in messages:
                ########################### PRIVOUS BOOK ############################
                book_body = books[-1].split(b'\x0110=')[0]
                book_body = book_body.split(b'\x01279')[1:]
                book_body = [b'\x01279'+ entry for entry in book_body]
                #####################################################################
                if secDesc in msg: # Check if msg containts security description
                    book_header = msg.split(b'\x01279')[0]
                    book_end = b'\x0110' + msg.split(b'\x0110')[-1]
                    msg_body = msg.split(b'\x0110=')[0]
                    msg_body = msg_body.split(b'\x01279')[1:]
                    msg_body = [b'\x01279'+ entry for entry in msg_body]
                    msg_body = [e if secDesc in e else None for e in msg_body]
                ############################ BOOK UPDATE  #############################
                    for entry in msg_body:
                        priceLevel = entry.split(b'\x011023=')[0]
                        entryType = entry[entry.find(b'\x01269=')+5:entry.find(b'\x01269=')+6]
                        actionType = entry[entry.find(b'\x01279=')+5:entry.find(b'\x01279=')+6]
                        deleted = entry.split(b'\x01270=')[0]+ b'\x01270=000000'
                        deleted = deleted + b'\x01271'+entry.split(b'\x01270=')[1].split(b'\x01271')[1]
                        if entryType == b'0': # BID tag 269=0
                            if actionType == b'1': # CHANGE 279=1
                                book_body[priceLevel-1] = entry
                            elif actionType == b'0': # NEW tag 279=0
                                if priceLevel == b'10':
                                    book_body[9] = entry
                                else:
                                    for i in reversed(range(priceLevel,10)):
                                        book_body[i] = book_body.pop(i-1)
                                    book_body[priceLevel-1] = entry
                            else:  # b'\x01279=2' DELETE
                                if priceLevel == b'10':
                                    book_body[9] = deleted
                                else:
                                    for i in range(priceLevel+1,10):
                                        book_body[i-1] = book_body.pop(i)
                                    book_body[9] = deleted
                        else: # OFFER tag 269=1
                            if actionType == b'1': # CHANGE 279=1
                                book_body[priceLevel+10] = entry
                            elif actionType == b'0': # NEW tag 279=0
                                if priceLevel == b'10':
                                    book_body[19] = entry
                                else:
                                    for i in reversed(range(priceLevel,10)):
                                        book_body[i] = book_body.pop(i-1)
                                    book_body[priceLevel+10] = entry
                            else:  # b'\x01279=2' DELETE
                                if priceLevel == b'10':
                                    book_body[19] = deleted
                                else:
                                    for i in range(priceLevel+1,10):
                                        book_body[i+10] = book_body.pop(i)
                                    book_body[19] = deleted

                    book_header += b''.join([e for e in book_body])
                    book = book_header + book_end
                    books.append(book)
        return books