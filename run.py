#! env/bin/python


import fixtools as fx

#path = "/home/jlroo/data/2012/XCME_MD_ES_20120102_20120106.gz"
path = "/home/jlroo/data/2010/XCME_MD_ES_20100104_20100108.gz"
#path = "/home/jlroo/data/2009/XCME_MD_ES_20090105_20090109.gz"
#path = "/home/jlroo/data/2009/XCME_MD_ES_20090126_20090130.gz"
#path = "/home/jlroo/data/2010/XCME_MD_ES_20100104_20100108.gz"

fixdata = fx.open_fix(path)
securities = fx.liquid_securities(fixdata)
fx.build_books(fixdata, securities, chunksize = 10**5)

