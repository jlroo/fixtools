#! env/bin/python


import fixtools as fx

path = "/home/jlroo/data/XCME_MD_ES_20120102_20120106.gz"
data = fx.open_fix(path)
securities = data.securities()
dates = data.dates
fut = fx.most_liquid(dates,"ES","fut")
opt = fx.most_liquid(dates,"ES","opt")
options = list(securities[opt]["OPT"].values())
future = list(securities[fut]["FUT"].values())
contracts = future+options
futures = fx.Futures(data.data)
options = fx.Options(data.data)
start = time.time()
books = fx.build_books(data.data,contracts)
end = time.time()
print(end - start)
