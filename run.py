#! env/bin/python


import fixtools as fx

path = "/home/jlroo/data/2012/XCME_MD_ES_20120102_20120106.gz"
fixdata = fx.open_fix(path)
securities = fx.liquid_securities(fixdata)
books = fx.build_books(fixdata, securities)

for sec_id in books.keys():
    with open(securities[sec_id].replace(" ","_"),'wb') as book_out:
        for book in books[sec_id]:
            book_out.write(book)
