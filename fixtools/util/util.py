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


def data_dates(fixdata, period="weekly"):
    peek = fixdata.data.peek(1).split(b"\n")[0]
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


def expiration_date(year, month, week, day=""):
    if day == "":
        day = "friday"
        print("Using Friday as expiration day. \n")
    weekday = {'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3, 'friday': 4, 'saturday': 5, 'sunday': 6}
    weeks = __calendar__.monthcalendar(year, month)
    for dd in weeks[week - 1]:
        date = __datetime__.datetime(year, month, dd)
        if date.weekday() == weekday[day.lower()]:
            if date.day // 7 == (week - 1):
                return __datetime__.datetime(year, month, dd)


def contract_code(month, codes="", cme_codes=False):
    if cme_codes:
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


def most_liquid(dates, instrument="", product="", year_code="", cme_codes=True, other_codes=""):
    codes = "F,G,H,J,K,M,N,Q,U,V,X,Z,F,G,H,J,K,M,N,Q,U,V,X,Z"
    if not cme_codes:
        codes = other_codes
    sec_code = ""
    date = __datetime__.datetime(year=dates[0].year, month=dates[0].month, day=dates[0].day)
    exp_week = next(filter(lambda day: settlement_day(day, 3, 'friday'), dates), None)
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
    sec_desc = instrument + sec_code + year_code
    return sec_desc


def liquid_securities( fixdata ,
                       instrument="ES" ,
                       group_code="EZ" ,
                       year_code="" ,
                       products=None ,
                       cme_codes=True ,
                       max_lines=50000 ):
    if products is None:
        products = ["FUT", "OPT"]
    securities = fixdata.securities(instrument, group_code, max_lines)
    dates = fixdata.dates
    liquid_secs = {}
    fut = most_liquid(dates, instrument, products[0], year_code, cme_codes)
    opt = most_liquid(dates, instrument, products[1], year_code, cme_codes)
    liquid_secs.update(securities[fut][products[0]])
    for price in securities[opt]["PAIRS"].keys():
        liquid_secs.update(securities[opt]['PAIRS'][price])
    return liquid_secs