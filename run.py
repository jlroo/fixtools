#! env/bin/python


import fixtools as fx

path = ""
data = fx.open_fix(path)
securities = data.securities()
dates = data.dates
fut = fx.most_liquid(dates,"ES","fut")
opt = fx.most_liquid(dates,"ES","opt")
options = list(securities[opt]["OPT"].values())
future = list(securities[fut]["FUT"].values())
contracts = future+options
books = fx.build_books(data,contracts)
