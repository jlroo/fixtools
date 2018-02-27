#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 24 12:50:00 2017

@author: jlroo
"""

from fixtools.core.book import OrderBook , DataBook


class Options:
    book = b''
    product = "options"
    top_order = 3
    sec_desc_id = b''

    def __init__( self , fixdata , securities , chunksize=10 ** 4 ):
        self.data_book = DataBook(data=fixdata.data , securities=securities , chunksize=chunksize)
        self.contracts_msgs = self.data_book.filter()

    def initial_book( self , security_id ):
        book_obj = OrderBook(self.contracts_msgs[security_id] , security_id , self.product)
        self.book = book_obj.initial_book()
        return self.book

    def build_book( self , path_out="" ):
        if path_out != "":
            self.data_book.create(path_out=path_out)
        else:
            books = self.data_book.create()
            return books
