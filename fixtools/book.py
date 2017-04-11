#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr  5 10:17:23 2017

@author: jlroo
"""

import multiprocessing as mp

SecurityID = ""

def __secFilter__(line):
    global SecurityID
    secDesc = b'\x0148='+SecurityID.encode()+b'\x01' in line
    mkRefresh = b'35=X\x01' in line
    if mkRefresh and secDesc:
        return line

class orderBook:

    book = ""
    top_order = 0

    def __init__(self,data,product):
        self.data = data
        self.product = product.lower()

    def initialbook(self,securityID):
        global SecurityID
        SecurityID = securityID
        secDescID = b'\x0148='+SecurityID.encode()+b'\x01'
        msgType = lambda e: b'35=X\x01' in e and secDescID in e
        tradeType = lambda e: e[e.find(b'\x01269=')+5:e.find(b'\x01269=')+6] in b'0|1'
        open_msg = lambda e: True if msgType(e) and tradeType(e) else None

        temp =  b'\x01279=NA\x0122=NA'+secDescID+b'83=NA\x01107=NA\x01269=0\x01270=NA\x01271=NA\x01273=NA\x01336=NA\x01346=NA\x011023='
        if self.product in "opt|options":
            self.top_order = 3
            prev_body = [temp+str(i).encode() for i in range(1,4)]+[temp+str(i).encode() for i in range(1,4)]
        if self.product in "fut|futures":
            self.top_order = 10
            temp.replace(b'\x01269=0',b'\x01269=1')
            prev_body = [temp+str(i).encode() for i in range(1,11)]+[temp+str(i).encode() for i in range(1,11)]

        msg = next(filter(open_msg,self.data), None)
        book_header = msg.split(b'\x01279')[0]
        book_end = b'\x0110' + msg.split(b'\x0110')[-1]
        msg_body = msg.split(b'\x0110=')[0].split(b'\x01279')[1:]
        msg_body = [b'\x01279'+ e for e in msg_body if secDescID in e and b'\x01276' not in e ]
        msg_body = iter(filter(lambda e: tradeType(e),msg_body))

    ######################## BOOK UPDATE  ###########################
        bids,offers = self.__update__(prev_body,msg_body)
        book_body = bids+offers
        book_header += b''.join([e for e in book_body])
        self.book = book_header + book_end
        self.data.seek(0)
        return self.book

    def buildbook(self,chunksize=10**4):
        global SecurityID
        secDescID = b'\x0148='+SecurityID.encode()+b'\x01'
        MsgSeqNum = lambda line:int(line.split(b'\x0134=')[1].split(b'\x01')[0])
        book = self.initialbook(SecurityID)
        bookSeqNum = int(book.split(b'\x0134=')[1].split(b'\x01')[0])
        updates = lambda entry: entry is not None and MsgSeqNum(entry)>bookSeqNum
        tradeType = lambda e: e[e.find(b'\x01269=')+5:e.find(b'\x01269=')+6] in b'0|1'
        with mp.Pool() as pool:
            msgMap = pool.imap(__secFilter__,self.data,chunksize)
            messages = iter(filter(updates,msgMap))
            for msg in messages:
            ########################## PRIVIOUS BOOK #############################
                prev_body = self.book.split(b'\x0110=')[0]
                prev_body = prev_body.split(b'\x01279')[1:]
                prev_body = [b'\x01279'+ entry for entry in prev_body]
            #####################################################################
                book_header = msg.split(b'\x01279')[0]
                book_end = b'\x0110' + msg.split(b'\x0110')[-1]
                msg_body = msg.split(b'\x0110=')[0].split(b'\x01279')[1:]
                msg_body = [b'\x01279'+ e for e in msg_body if secDescID in e and b'\x01276' not in e ]
                msg_body = iter(filter(lambda e: tradeType(e),msg_body))
            ############################ BOOK UPDATE  ###########################
                bids,offers = self.__update__(prev_body,msg_body)
                book_body = bids+offers
                if book_body == prev_body:
                    pass
                else:
                    book_header += b''.join([e for e in book_body])
                    self.book = book_header + book_end
                    yield self.book
        self.data.seek(0)

    def __update__(self,book_body,msg_body):
        bids,offers = book_body[0:self.top_order],book_body[self.top_order:]
        for entry in msg_body:
            try:
                priceLevel = int(entry.split(b'\x011023=')[1])
                entryType = int(entry[entry.find(b'\x01269=')+5:entry.find(b'\x01269=')+6])
                actionType = int(entry[entry.find(b'\x01279=')+5:entry.find(b'\x01279=')+6])
                if entryType == 0: # BID tag 269= esh9[1]
                    if actionType == 1: # CHANGE 279=1
                        bids[priceLevel-1] = entry
                    elif actionType == 0: # NEW tag 279=0
                        if priceLevel == self.top_order:
                            bids[self.top_order-1] = entry
                        else:
                            bids.insert(priceLevel-1,entry)
                            for i in range(priceLevel,self.top_order):
                                bids[i] = bids[i].replace(b'\x011023='+str(i).encode(),b'\x011023='+str(i+1).encode())
                            bids.pop()
                    else:  # b'\x01279=2' DELETE
                        delete = entry.split(b'\x011023=')[0]+b'\x011023=' + str(self.top_order).encode()
                        if priceLevel == self.top_order:
                            bids[self.top_order-1] = delete
                        else:
                            bids.pop(priceLevel-1)
                            for i in range(priceLevel,self.top_order):
                                bids[i-1] = bids[i-1].replace(b'\x011023='+str(i+1).encode(),b'\x011023='+str(i).encode())
                            bids.append(delete)
                else: # OFFER tag 269=1
                    if actionType == 1: # CHANGE 279=1
                        offers[priceLevel-1] = entry
                    elif actionType == 0: # NEW tag 279=0
                        if priceLevel == self.top_order:
                            offers[self.top_order-1] = entry
                        else:
                            offers.insert(priceLevel-1,entry)
                            for i in range(priceLevel,self.top_order):
                                offers[i] = offers[i].replace(b'\x011023='+str(i).encode(),b'\x011023='+str(i+1).encode())
                            offers.pop()
                    else:  # b'\x01279=2' DELETE
                        delete = entry.split(b'\x011023=')[0]+b'\x011023='+ str(self.top_order).encode()
                        if priceLevel == self.top_order:
                            offers[self.top_order-1] = delete
                        else:
                            offers.pop(priceLevel-1)
                            for i in range(priceLevel,self.top_order):
                                offers[i-1] = offers[i-1].replace(b'\x011023='+str(i+1).encode(),b'\x011023='+str(i).encode())
                            offers.append(delete)
            except StopIteration:
                continue
        return bids,offers