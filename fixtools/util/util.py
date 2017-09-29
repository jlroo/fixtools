"""
Created on Fri Jul 22 17:33:13 2016
@author: jlroo
"""

import bz2 as __bz2__
import calendar as __calendar__
import datetime as __datetime__
import gzip as __gzip__
import multiprocessing as __mp__
from fixtools.core.book import OrderBook
from fixtools.io.fixfast import FixData
from collections import defaultdict

"""
					Def FixData

This class returns that number a report of the fix data
contracts and volume.

"""


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


def contract_code(month, codes=""):
    if codes == "":
        codes = "F,G,H,J,K,M,N,Q,U,V,X,Z,F,G,H,J,K,M,N,Q,U,V,X,Z"
        print("Using CME Codes: \n" + codes)
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


def most_liquid(dates, instrument="", product=""):
    date = __datetime__.datetime(year=dates[0].year, month=dates[0].month, day=dates[0].day)
    contract_year = lambda yr: yr[-1:] if yr[1:3] != "00" else yr[-1:]
    exp_week = next(filter(lambda day: settlement_day(day, 3, 'friday'), dates), None)
    expired = True if date.day > 16 else False
    if exp_week is not None or expired:
        if product.lower() in ("fut", "futures"):
            if date.month % 3 == 0:
                sec_code = contract_code(date.month + 3)
            if date.month % 3 == 1:
                sec_code = contract_code(date.month + 2)
            if date.month % 3 == 2:
                sec_code = contract_code(date.month + 1)
        if product.lower() in ("opt", "options"):
            sec_code = contract_code(date.month + 1)
    else:
        if product.lower() in ("fut", "futures"):
            if date.month % 3 == 0:
                sec_code = contract_code(date.month)
            if date.month % 3 == 1:
                sec_code = contract_code(date.month + 2)
            if date.month % 3 == 2:
                sec_code = contract_code(date.month + 1)
        if product.lower() in ("opt", "options"):
            sec_code = contract_code(date.month)
    sec_desc = instrument + sec_code + contract_year(str(date.year))
    return sec_desc


def liquid_securities(fixdata, instrument = "ES", group_code = "EZ", max_lines = 10000):
    global liquid_secs
    securities = fixdata.securities(instrument, group_code, max_lines)
    dates = fixdata.dates
    liquid_secs = {}
    fut = most_liquid(dates,instrument,"fut")
    opt = most_liquid(dates,instrument,"opt")
    liquid_secs.update(securities[fut]["FUT"])
    for price in securities[opt]["PAIRS"].keys():
        liquid_secs.update(securities[opt]['PAIRS'][price])
    return liquid_secs


def __filter__(line):
    valid_contract = [sec if sec in line else None for sec in __secdesc__]
    setids = filter(None,valid_contract)
    security_ids = set(int(sec.split(b'\x0148=')[1].split(b'\x01')[0]) for sec in setids)
    if b'35=X\x01' in line and any(valid_contract):
        return (security_ids, line)


def filter_securities(data, contracts, chunksize = 10 ** 4):
    messages = defaultdict(list)
    global __secdesc__
    __secdesc__ = [b'\x0148=' + str(sec_id).encode() + b'\x01' for sec_id in contracts]
    with __mp__.Pool() as pool:
        filtered = pool.map(__filter__, data, chunksize)
        for setids, line in filter(None, filtered):
            for secid in setids:
                messages[secid].append(line)
    return messages


def __books__(sec_id):
    books = []
    product = lambda sec_desc: "opt" if len(sec_desc) > 7 else "fut"
    book_obj = OrderBook(contracts_msgs[sec_id], sec_id, product(liquid_secs[sec_id]))
    for book in book_obj.build_book():
        books.append(book)
    return {sec_id: books}


def __booksOut__(sec_id):
    product = lambda sec_desc: "opt" if len(sec_desc) > 7 else "fut"
    book_obj = OrderBook(contracts_msgs[sec_id], sec_id, product(liquid_secs[sec_id]))
    filename = liquid_secs[sec_id].replace(" ","_")
    with open(__out__ + filename,'wb') as book_out:
        for book in book_obj.build_book():
            book_out.write(book)


def build_books(fixdata, securities, file_out = True, path_out = "", chunksize = 10 ** 4):
    global contracts_msgs
    global __out__
    books = {}
    __contracts__ = set(securities.keys())
    contracts_msgs = filter_securities(fixdata.data, __contracts__, chunksize)
    if file_out == False:
        with __mp__.Pool() as pool:
            books = pool.map(__books__,__contracts__)
        return books
    if file_out == True:
        if path_out != "":
            if path_out[:-1] != "/":
                __out__ = path_out + "/"
        with __mp__.Pool() as pool:
            books = pool.map(__booksOut__,__contracts__)
    try:
        fixdata.data.seek(0)
    except AttributeError:
        pass


def __secfilter__(line):
    global __securityID__
    sec_desc = b'\x0148=' + __securityID__.encode() + b'\x01' in line
    mk_refresh = b'35=X\x01' in line
    if mk_refresh and sec_desc:
        return line


def initial_book(data, security_id, product):
    sec_desc_id = b'\x0148=' + security_id.encode() + b'\x01'
    msg_type = lambda e: e is not None and b'35=X\x01' in e and sec_desc_id in e
    trade_type = lambda e: e is not None and e[e.find(b'\x01269=') + 5:e.find(b'\x01269=') + 6] in b'0|1'
    open_msg = lambda e: msg_type(e) and trade_type(e)
    temp = b'\x01279=NA\x0122=NA' + sec_desc_id + \
           b"83=NA\x01107=NA\x01269=0\x01270=NA\x01271=NA\x01273=NA\x01336=NA\x01346=NA\x011023="
    if product in "opt|options":
        top_order = 3
        prev_body = [temp + str(i).encode() for i in range(1, top_order + 1)]
        temp = temp.replace(b'\x01269=0', b'\x01269=1')
        prev_body = prev_body + [temp + str(i).encode() for i in range(1, top_order + 1)]
    if product in "fut|futures":
        top_order = 10
        prev_body = [temp + str(i).encode() for i in range(1, top_order + 1)]
        temp = temp.replace(b'\x01269=0', b'\x01269=1')
        prev_body = prev_body + [temp + str(i).encode() for i in range(1, top_order + 1)]
    msg = next(filter(open_msg, data), None)
    book_header = msg.split(b'\x01279')[0]
    book_end = b'\x0110' + msg.split(b'\x0110')[-1]
    msg_body = msg.split(b'\x0110=')[0].split(b'\x01279')[1:]
    msg_body = [b'\x01279' + e for e in msg_body if sec_desc_id in e and b'\x01276' not in e]
    msg_body = iter(filter(lambda e: trade_type(e), msg_body))
    # BOOK UPDATE
    bids, offers = __update__(prev_body, msg_body, sec_desc_id, top_order)
    book_body = bids + offers
    book_header += b''.join([e for e in book_body])
    book = book_header + book_end
    return book


def build_book(prev_book, update_msg, security_id, top_order):
    sec_desc_id = b'\x0148=' + security_id.encode() + b'\x01'
    trade_type = lambda e: e[e.find(b'\x01269=') + 5:e.find(b'\x01269=') + 6] in b'0|1'
    prev_body = prev_book.split(b'\x0110=')[0]
    prev_body = prev_body.split(b'\x01279')[1:]
    prev_body = [b'\x01279' + entry for entry in prev_body]
    book_header = update_msg.split(b'\x01279')[0]
    book_end = b'\x0110' + update_msg.split(b'\x0110')[-1]
    msg_body = update_msg.split(b'\x0110=')[0].split(b'\x01279')[1:]
    msg_body = [b'\x01279' + e for e in msg_body if sec_desc_id in e and b'\x01276' not in e]
    msg_body = iter(filter(lambda e: trade_type(e), msg_body))
    # BOOK UPDATE
    bids, offers = __update__(prev_body, msg_body, sec_desc_id, top_order)
    book_body = bids + offers
    if book_body == prev_body:
        book = None
    else:
        book_header += b''.join([e for e in book_body])
        book = book_header + book_end
    return book


def __update__(book_body, msg_body, sec_desc_id, top_order):
    bids, offers = book_body[0:top_order], book_body[top_order:]
    for entry in msg_body:
        try:
            price_level = int(entry.split(b'\x011023=')[1])
            entry_type = int(entry[entry.find(b'\x01269=') + 5:entry.find(b'\x01269=') + 6])
            action_type = int(entry[entry.find(b'\x01279=') + 5:entry.find(b'\x01279=') + 6])
            temp = b'\x01279=NA\x0122=NA' + sec_desc_id + b'83=NA\x01107=NA\x01269=0\x01270=NA\x01271=NA\x01273=NA\x01336=NA\x01346=NA\x011023='
            if entry_type == 0:  # BID tag 269= esh9[1]
                if action_type == 1:  # CHANGE 279=1
                    bids[price_level - 1] = entry
                elif action_type == 0:  # NEW tag 279=0
                    if price_level == top_order:
                        bids[top_order - 1] = entry
                    else:
                        bids.insert(price_level - 1, entry)
                        for i in range(price_level, top_order):
                            bids[i] = bids[i].replace(b'\x011023=' + str(i).encode(),
                                                      b'\x011023=' + str(i + 1).encode())
                        bids.pop()
                else:  # b'\x01279=2' DELETE
                    delete = temp + str(top_order).encode()
                    if price_level == top_order:
                        bids[top_order - 1] = delete
                    else:
                        bids.pop(price_level - 1)
                        for i in range(price_level, top_order):
                            bids[i - 1] = bids[i - 1].replace(b'\x011023=' + str(i + 1).encode(),
                                                              b'\x011023=' + str(i).encode())
                        bids.append(delete)
            else:  # OFFER tag 269=1
                if action_type == 1:  # CHANGE 279=1
                    offers[price_level - 1] = entry
                elif action_type == 0:  # NEW tag 279=0
                    if price_level == top_order:
                        offers[top_order - 1] = entry
                    else:
                        offers.insert(price_level - 1, entry)
                        for i in range(price_level, top_order):
                            offers[i] = offers[i].replace(b'\x011023=' + str(i).encode(),
                                                          b'\x011023=' + str(i + 1).encode())
                        offers.pop()
                else:  # b'\x01279=2' DELETE
                    temp = temp.replace(b'\x01269=0', b'\x01269=1')
                    delete = temp + str(top_order).encode()
                    if price_level == top_order:
                        offers[top_order - 1] = delete
                    else:
                        offers.pop(price_level - 1)
                        for i in range(price_level, top_order):
                            offers[i - 1] = offers[i - 1].replace(b'\x011023=' + str(i + 1).encode(),
                                                                  b'\x011023=' + str(i).encode())
                        offers.append(delete)
        except StopIteration:
            continue
    return bids, offers
