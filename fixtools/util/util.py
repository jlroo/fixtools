"""
Created on Fri Jul 22 17:33:13 2016
@author: jlroo
"""

import bz2 as __bz2__
import calendar as __calendar__
import datetime as __datetime__
import gzip as __gzip__
from fixtools.io.fixfast import FixData


# 					Def FixData
#
# This class returns that number a report of the fix data
# contracts and volume.

def open_fix(path, period="weekly", compression=True):
    if period.lower() not in ("weekly", "daily", "monthly"):
        raise ValueError("Supported time period: weekly or daily")
    src = {"path": path, "period": period.lower()}
    if compression is False:
        if path[-4:].lower in (".zip", ".tar"):
            raise ValueError("Supported compressions gzip, bz2 or bytes data")
        else:
            fixfile = open(path, 'rb')
    else:
        if path[-3:] == ".gz":
            fixfile = __gzip__.open(path, 'rb')
        elif path[-4:] == ".bz2":
            fixfile = __bz2__.BZ2File(path, 'rb')
        else:
            raise ValueError("Supported files gzip,bz2, uncompress bytes file. \
            For uncompressed files change compression flag to False.")
    return FixData(fixfile, src)


def data_dates( data_line , period="weekly" ):
    peek = data_line.split(b"\n")[0]
    day0 = peek[peek.find(b'\x0152=') + 4:peek.find(b'\x0152=') + 12]
    start = __datetime__.datetime(year=int(day0[:4]), month=int(day0[4:6]), day=int(day0[6:8]))
    if period == "weekly":
        dates = [start + __datetime__.timedelta(days=i) for i in range(6)]
        return dates


def settlement_day(date, week_number, day_of_week):
    weekday = {'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3, 'friday': 4, 'saturday': 5, 'sunday': 6}
    date = __datetime__.datetime(date.year, date.month, date.day)
    if date.weekday() == weekday[day_of_week.lower()]:
        if date.day // 7 == (week_number - 1):
            return True
    return False


def expiration_date( year=None , month=None , week=None , day=None ):
    if not day:
        day = "friday"
    weekday = {'monday': 0 , 'tuesday': 1 , 'wednesday': 2 ,
               'thursday': 3 , 'friday': 4 , 'saturday': 5 , 'sunday': 6}
    weeks = __calendar__.monthcalendar(year, month)
    exp_day = week - 1
    if weeks[0][-1] == 1:
        exp_week = week
    else:
        exp_week = week - 1
    for dd in weeks[exp_week]:
        date = __datetime__.datetime(year, month, dd)
        if date.weekday() == weekday[day.lower()]:
            if date.day // 7 == exp_day:
                return date


def contract_code( month=None , codes=None , cme_codes=None ):
    if not cme_codes:
        codes = "F,G,H,J,K,M,N,Q,U,V,X,Z,F,G,H,J,K,M,N,Q,U,V,X,Z"
    month_codes = {k[0]: k[1] for k in enumerate(codes.rsplit(","), 1)}
    codes_hash = {}
    for index in month_codes:
        if index % 3 == 0:
            codes_hash[index] = (
                month_codes[index], {index - 2: month_codes[index - 2], index - 1: month_codes[index - 1]})
    if month % 3 == 0:
        return codes_hash[month][0]
    if month % 3 == 1:
        return codes_hash[month + 2][1][month]
    if month % 3 == 2:
        return codes_hash[month + 1][1][month]


def most_liquid( data_line , instrument=None , product=None , code_year=None , cme_codes=None , other_codes=None ):
    day0 = data_line[data_line.find(b'\x0152=') + 4:data_line.find(b'\x0152=') + 12]
    start = __datetime__.datetime(year=int(day0[:4]) , month=int(day0[4:6]) , day=int(day0[6:8]))
    dates = [start + __datetime__.timedelta(days=i) for i in range(6)]
    if cme_codes:
        codes = "F,G,H,J,K,M,N,Q,U,V,X,Z,F,G,H,J,K,M,N,Q,U,V,X,Z"
    else:
        codes = other_codes
    sec_code = ""
    date = __datetime__.datetime(year=dates[0].year, month=dates[0].month, day=dates[0].day)
    exp_week = filter(lambda day: settlement_day(day , 3 , 'friday') , dates)
    expired = True if date.day > 16 else False
    if exp_week is not None or expired:
        if product.lower() in ("fut", "futures"):
            if date.month % 3 == 0:
                sec_code = contract_code(date.month + 3, codes)
            if date.month % 3 == 1:
                sec_code = contract_code(date.month + 2, codes)
            if date.month % 3 == 2:
                sec_code = contract_code(date.month + 1, codes)
        if product.lower() in ("opt", "options"):
            sec_code = contract_code(date.month + 1, codes)
    else:
        if product.lower() in ("fut", "futures"):
            if date.month % 3 == 0:
                sec_code = contract_code(date.month, codes)
            if date.month % 3 == 1:
                sec_code = contract_code(date.month + 2, codes)
            if date.month % 3 == 2:
                sec_code = contract_code(date.month + 1, codes)
        if product.lower() in ("opt", "options"):
            sec_code = contract_code(date.month, codes)
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


def liquid_securities( fixdata=None , instrument=None , group_code=None , code_year=None , products=None ,
                       cme_codes=True , max_lines=50000 ):
    if products is None:
        products = ["FUT", "OPT"]
    if instrument is None:
        instrument = "ES"
    if group_code is None:
        group_code = "EZ"
    description = security_description(fixdata , instrument , group_code , max_lines)
    securities = contracts(description)
    liquid = {}
    fix_line = fixdata[0].split(b"\n")[0]
    fut = most_liquid(fix_line , instrument , products[0] , code_year , cme_codes)
    opt = most_liquid(fix_line , instrument , products[1] , code_year , cme_codes)
    liquid.update(securities[fut][products[0]])
    for price in securities[opt]["PAIRS"].keys():
        liquid.update(securities[opt]['PAIRS'][price])
    return liquid
