#! env/bin/python


import fixtools as fx


path = "/home/jlroo/data/2012/XCME_MD_ES_20120102_20120106.gz"
path = "/home/jlroo/data/2010/XCME_MD_ES_20100104_20100108.gz"
path = "/home/jlroo/data/2009/XCME_MD_ES_20090105_20090109.gz"
path = "/home/jlroo/data/2009/XCME_MD_ES_20090126_20090130.gz"
path = "/home/jlroo/data/2010/XCME_MD_ES_20100104_20100108.gz"

fixdata = fx.open_fix(path)

securities = fx.liquid_securities(fixdata)

securities = {31747: 'ESH2', 173315:'ESF2 C1275', 
              564823:'ESF2 P1275', 78747:'ESF2 C1280',
              67428:'ESF2 P1800'}

books = fx.build_books(fixdata, securities)

for sec_id in books.keys():
    with open(securities[sec_id].replace(" ","_"),'wb') as book_out:
        for book in books[sec_id]:
            book_out.write(book)
