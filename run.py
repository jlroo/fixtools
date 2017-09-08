#! env/bin/python


import fixtools as fx

path = "/home/jlroo/data/2012/XCME_MD_ES_20120102_20120106.gz"
path = "/home/jlroo/data/2010/XCME_MD_ES_20100104_20100108.gz"
path = "/home/jlroo/data/2009/XCME_MD_ES_20090105_20090109.gz"
path = "/home/jlroo/data/2009/XCME_MD_ES_20090126_20090130.gz"

path = "/home/jlroo/data/2010/XCME_MD_ES_20100104_20100108.gz"
fixdata = fx.open_fix(path)

securities = fx.liquid_securities(fixdata)

books = fx.build_books(fixdata, securities)

for sec_id in books.keys():
    with open(securities[sec_id].replace(" ","_"),'wb') as book_out:
        for book in books[sec_id]:
            book_out.write(book)

securities = fixdata.securities()
            
pairs = {}
contracts = {}
months = set("F,G,H,J,K,M,N,Q,U,V,X,Z".split(","))
for line in fixdata.data:
    desc = line[line.find(b'35=d\x01') + 3:line.find(b'35=d\x01') + 4]
    if desc != b'd':
        break
    sec_id = int(line.split(b'\x0148=')[1].split(b'\x01')[0])
    sec_desc = line.split(b'\x01107=')[1].split(b'\x01')[0].decode()
    sec_key = sec_desc[0:4]
    if sec_key not in contracts.keys():
        contracts[sec_key] = {"FUT": {}, "OPT": {}, "PAIRS": {}, "SPREAD": {}}
    for month in months:
        if month in sec_desc:
            if len(sec_desc) <contracts.keys():
                contracts[sec_key]['FUT'][sec_id] = sec_desc
            if 'P' in sec_desc or 'C' in sec_desc:
                contracts[sec_key]['OPT'][sec_id] = sec_desc
                if 'C' in sec_desc:
                    call_price = int(sec_desc.split(" C")[-1])
                    if call_price not in contracts[sec_key]['PAIRS'].keys():
                        contracts[sec_key]['PAIRS'][call_price] = {}
                        contracts[sec_key]['PAIRS'][call_price][sec_id] = sec_desc
                    else:
                        contracts[sec_key]['PAIRS'][call_price][sec_id] = sec_desc
                if "P" in sec_desc:
                    put_price = int(sec_desc.split(" P")[-1])
                    if put_price not in contracts[sec_key]['PAIRS'].keys():
                        contracts[sec_key]['PAIRS'][put_price] = {}
                        contracts[sec_key]['PAIRS'][put_price][sec_id] = sec_desc
                    else:
                        contracts[sec_key]['PAIRS'][put_price][sec_id] = sec_desc
            if '-' in sec_desc:
                contracts[sec_key]['SPREAD'][sec_id] = sec_desc



pairs = {}
contracts = {}
months = set("F,G,H,J,K,M,N,Q,U,V,X,Z".split(","))
for line in fixdata.data:
    desc = line[line.find(b'35=d\x01') + 3:line.find(b'35=d\x01') + 4]
    if desc != b'd':
        break
    sec_id = int(line.split(b'\x0148=')[1].split(b'\x01')[0])
    sec_desc = line.split(b'\x01107=')[1].split(b'\x01')[0].decode()
    sec_key = sec_desc[0:4]
    if sec_key not in contracts.keys():
        contracts[sec_key] = {"FUT": {}, "OPT": {}, "PAIRS": {}, "SPREAD": {}}
    if sec_key not in pairs.keys():
        pairs[sec_key] = {}
    for month in months:
        if month in sec_desc:
            if len(sec_desc) < 7:
                contracts[sec_key]['FUT'][sec_id] = sec_desc
            if 'P' in sec_desc or 'C' in sec_desc:
                contracts[sec_key]['OPT'][sec_id] = sec_desc
                if 'C' in sec_desc:
                    call_price = int(sec_desc.split(" C")[-1])
                    if call_price not in pairs[sec_key].keys():
                        pairs[sec_key][call_price] = {}
                        pairs[sec_key][call_price][sec_id] = sec_desc
                    else:
                        pairs[sec_key][call_price][sec_id] = sec_desc
                if "P" in sec_desc:
                    put_price = int(sec_desc.split(" P")[-1])
                    if put_price not in pairs[sec_key].keys():
                        pairs[sec_key][put_price] = {}
                        pairs[sec_key][put_price][sec_id] = sec_desc
                    else:
                        pairs[sec_key][put_price][sec_id] = sec_desc
            if '-' in sec_desc:
                contracts[sec_key]['SPREAD'][sec_id] = sec_desc

for sec_key in contracts.keys():
    for price in pairs[sec_key].keys():
        contracts[sec_key]['PAIRS'][price] = {}
        if len(pairs[sec_key][price])==2:
            contracts[sec_key]['PAIRS'][price].update(pairs[sec_key][price])
            
            
            
d = {}
for price in contracts['ESF0']['PAIRS'].keys():
    d.update(contracts['ESF0']['PAIRS'][price])