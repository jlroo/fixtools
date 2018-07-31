"""
Created on Fri Jul 20 11:35:00 2018
@author: jlroo

"""

import datetime as __datetime__
import calendar as __calendar__


def settlement_day( date , week_number , day_of_week ):
    weekday = {'monday': 0 , 'tuesday': 1 , 'wednesday': 2 , 'thursday': 3 , 'friday': 4 , 'saturday': 5 , 'sunday': 6}
    date = __datetime__.datetime(date.year , date.month , date.day)
    if date.weekday() == weekday[day_of_week.lower()]:
        if date.day // 7 == (week_number - 1):
            return True
    return False


def expiration_date( year=None , month=None , week=None , day=None ):
    if not day:
        day = "friday"
    weekday = {'monday': 0 , 'tuesday': 1 , 'wednesday': 2 ,
               'thursday': 3 , 'friday': 4 , 'saturday': 5 , 'sunday': 6}
    weeks = __calendar__.monthcalendar(year , month)
    exp_day = week - 1
    if weeks[0][-1] == 1:
        exp_week = week
    else:
        exp_week = week - 1
    for dd in weeks[exp_week]:
        date = __datetime__.datetime(year , month , dd)
        if date.weekday() == weekday[day.lower()]:
            if date.day // 7 == exp_day:
                return date


def contract_code( month=None , codes=None , cme_codes=None ):
    if not cme_codes:
        codes = "F,G,H,J,K,M,N,Q,U,V,X,Z,F,G,H,J,K,M,N,Q,U,V,X,Z"
    month_codes = {k[0]: k[1] for k in enumerate(codes.rsplit(",") , 1)}
    codes_hash = {}
    for index in month_codes:
        if index % 3 == 0:
            codes_hash[index] = (
                month_codes[index] , {index - 2: month_codes[index - 2] , index - 1: month_codes[index - 1]})
    if month % 3 == 0:
        return codes_hash[month][0]
    if month % 3 == 1:
        return codes_hash[month + 2][1][month]
    if month % 3 == 2:
        return codes_hash[month + 1][1][month]


def most_liquid( data_line , instrument=None , product=None , code_year=None , cme_codes=True , other_codes=None ):
    day0 = data_line[data_line.find(b'\x0152=') + 4:data_line.find(b'\x0152=') + 12]
    start = __datetime__.datetime(year=int(day0[:4]) , month=int(day0[4:6]) , day=int(day0[6:8]))
    dates = [start + __datetime__.timedelta(days=i) for i in range(6)]
    codes = "F,G,H,J,K,M,N,Q,U,V,X,Z,F,G,H,J,K,M,N,Q,U,V,X,Z"
    if not cme_codes:
        codes = other_codes
    sec_code = ""
    date = __datetime__.datetime(year=dates[0].year , month=dates[0].month , day=dates[0].day)
    exp_week = filter(lambda day: settlement_day(day , 3 , 'friday') , dates)
    expired = True if date.day > 16 else False
    if exp_week is not None or expired:
        if product.lower() in ("fut" , "futures"):
            if date.month % 3 == 0:
                sec_code = contract_code(date.month + 3 , codes)
            if date.month % 3 == 1:
                sec_code = contract_code(date.month + 2 , codes)
            if date.month % 3 == 2:
                sec_code = contract_code(date.month + 1 , codes)
        if product.lower() in ("opt" , "options"):
            sec_code = contract_code(date.month + 1 , codes)
    else:
        if product.lower() in ("fut" , "futures"):
            if date.month % 3 == 0:
                sec_code = contract_code(date.month , codes)
            if date.month % 3 == 1:
                sec_code = contract_code(date.month + 2 , codes)
            if date.month % 3 == 2:
                sec_code = contract_code(date.month + 1 , codes)
        if product.lower() in ("opt" , "options"):
            sec_code = contract_code(date.month , codes)
    sec_desc = instrument + sec_code + code_year
    return sec_desc


def security_description( data , group="ES" , group_code="EZ" , max_lines=10000 ):
    description = []
    code = (group.encode() , group_code.encode())
    for cnt , line in enumerate(data):
        if cnt > max_lines:
            break
        desc = line[line.find(b'35=d\x01') + 3:line.find(b'35=d\x01') + 4]
        tag_sec_group = b'\x011151='
        tag_grp_code = b'\x0155='
        sec_grp = line[line.find(tag_sec_group) + 6:line.find(tag_sec_group) + 8]
        code_grp = line[line.find(tag_grp_code) + 4:line.find(tag_grp_code) + 6]
        if desc == b'd' and sec_grp in code and code_grp in code:
            sec_id = int(line.split(b'\x0148=')[1].split(b'\x01')[0])
            sec_desc = line.split(b'\x01107=')[1].split(b'\x01')[0].decode()
            description.append({sec_id: sec_desc})
    try:
        data.close()
    except AttributeError:
        pass
    return description


def contracts( description ):
    securities = {}
    months = set("F,G,H,J,K,M,N,Q,U,V,X,Z".split(","))
    filtered = {list(item.keys())[0]: list(item.values())[0] for item in description}
    for sec_id in filtered.keys():
        sec_desc = filtered[sec_id]
        if len(sec_desc) < 12:
            sec_key = sec_desc[0:4]
            if sec_key not in securities.keys():
                securities[sec_key] = {"FUT": {} , "OPT": {} , "PAIRS": {} , "SPREAD": {}}
            for month in months:
                if month in sec_desc:
                    if len(sec_desc) < 7:
                        securities[sec_key]['FUT'][sec_id] = sec_desc
                    if 'P' in sec_desc or 'C' in sec_desc:
                        securities[sec_key]['OPT'][sec_id] = sec_desc
                        if 'C' in sec_desc:
                            call_price = int(sec_desc.split(" C")[-1])
                            if call_price not in securities[sec_key]['PAIRS'].keys():
                                securities[sec_key]['PAIRS'][call_price] = {}
                                securities[sec_key]['PAIRS'][call_price][sec_id] = sec_desc
                            else:
                                securities[sec_key]['PAIRS'][call_price][sec_id] = sec_desc
                        if "P" in sec_desc:
                            put_price = int(sec_desc.split(" P")[-1])
                            if put_price not in securities[sec_key]['PAIRS'].keys():
                                securities[sec_key]['PAIRS'][put_price] = {}
                                securities[sec_key]['PAIRS'][put_price][sec_id] = sec_desc
                            else:
                                securities[sec_key]['PAIRS'][put_price][sec_id] = sec_desc
                    if '-' in sec_desc:
                        securities[sec_key]['SPREAD'][sec_id] = sec_desc
    for sec_key in securities.keys():
        pairs = securities[sec_key]['PAIRS'].copy()
        for price in pairs.keys():
            if len(pairs[price]) != 2:
                del securities[sec_key]['PAIRS'][price]
    return securities


def liquid_securities( fixdata=None , instrument=None , group_code=None , year_code=None ,
                       products=None , cme_codes=True , max_lines=50000 ):
    if products is None:
        products = ["FUT" , "OPT"]
    if instrument is None:
        instrument = "ES"
    if group_code is None:
        group_code = "EZ"
    description = security_description(fixdata , instrument , group_code , max_lines)
    securities = contracts(description)
    liquid = {}
    fix_line = fixdata[0].split(b"\n")[0]
    fut = most_liquid(fix_line , instrument , products[0] , year_code , cme_codes)
    opt = most_liquid(fix_line , instrument , products[1] , year_code , cme_codes)
    liquid.update(securities[fut][products[0]])
    for price in securities[opt]["PAIRS"].keys():
        liquid.update(securities[opt]['PAIRS'][price])
    return liquid


def line_filter( line ):
    valid_contract = [sec if sec in line else None for sec in security_desc]
    if b'35=X\x01' in line and any(valid_contract):
        set_ids = filter(None , valid_contract)
        security_ids = set(int(sec.split(b'\x0148=')[1].split(b'\x01')[0]) for sec in set_ids)
        return security_ids , line


if __name__ == "__main__":
    from pyspark import SparkConf , SparkContext
    import tacc_utils as tx

    # In Jupyter you have to stop the current context first
    sc.stop()

    # Create new config
    conf = (SparkConf()
            .set("spark.driver.maxResultSize" , "23g"))

    # Create new context
    sc = SparkContext(conf=conf)

    path = "cme/01/XCME_MD_ES_20100104_20100108"
    fixfile = sc.textFile(path)
    data_lines = fixfile.take(10000)

    year_code = '0'
    opt_code = tx.most_liquid(data_line=data_lines[0] , instrument="ES" , product="OPT" , year_code=year_code)
    fut_code = tx.most_liquid(data_line=data_lines[0] , instrument="ES" , product="FUT" , year_code=year_code)
    liquid_secs = tx.liquid_securities(data_lines , year_code='0')
    contract_ids = set(liquid_secs.keys())
    security_desc = [b'\x0148=' + str(sec_id).encode() + b'\x01' for sec_id in contract_ids]

"""    
    import os
    path = "/Users/jlroo/space/09-RESEARCH/emini-sp/data/raw/2010/XCME_MD_ES_20100104_20100108"
    data = []
    with open(path, 'rb') as file:
        for cnt, line in enumerate(file):
            if cnt == 500000:
                break
            data.append(line)

    folder = "/Users/jlroo/space/09-RESEARCH/emini-sp/data/raw/"
    files = os.listdir(folder)
    for k , file in enumerate(files):
        data_out = "/Users/jlroo/cme/"
        year_code = '0'
        opt_code = most_liquid(data_line=data_line, instrument="ES", product="OPT", year_code=year_code)
        fut_code = most_liquid(data_line=data_line, instrument="ES", product="FUT", year_code=year_code)
        liquid_secs = liquid_securities(data_lines, year_code='0')
        contract_ids = set(liquid_secs.keys())
        security_desc = [b'\x0148=' + str(sec_id).encode() + b'\x01' for sec_id in contract_ids]
        filtered = data.filter(line_filter)
        # <!--- spark parallel section --->
        #
        # data_book(data=fixdata.data , securities=securities , path=path_out , chunksize=chunksize)
        #
        # <!--- spark parallel section -->
        for sec_desc in liquid_secs.values():
            name = path_out + sec_desc.replace(" " , "-")
            print("[DONE]  -- CONTRACT -- " + name)

"""
