"""
Created on Fri Jul 22 17:33:13 2016
@author: jlroo
"""

import gzip
import bz2
import re
import datetime
import calendar
from fixtools.fixfast import FixData

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
			fixfile = gzip.open(path, 'rb')
		elif path[-4:] == ".bz2":
			fixfile = bz2.BZ2File(path, 'rb')
		else:
			raise ValueError("Supported files gzip,bz2, \
			uncompress bytes file. For uncompressed \
			files change compression flag to False.")
	return FixData(fixfile, src)


def settlement_day(date, week_number, day_of_week):
	weekday = {'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3, 'friday': 4, 'saturday': 5, 'sunday': 6}
	date = datetime.datetime(date.year, date.month, date.day)
	if date.weekday() == weekday[day_of_week.lower()]:
		if date.day // 7 == (week_number - 1):
			return True
	return False


def expiration_date(year, month, week, day=""):
	if day == "":
		day = "friday"
		print("Using Friday as expiration day. \n")
	weekday = {'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3, 'friday': 4, 'saturday': 5, 'sunday': 6}
	weeks = calendar.monthcalendar(year, month)
	for dd in weeks[week - 1]:
		date = datetime.datetime(year, month, dd)
		if date.weekday() == weekday[day.lower()]:
			if date.day // 7 == (week - 1):
				return datetime.datetime(year, month, dd)


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
	date = datetime.datetime(year=dates[0].year, month=dates[0].month, day=dates[0].day)
	contract_year = lambda yr: yr[-1:] if yr[1:3] != "00" else yr[-1:]
	exp_week = next(filter(lambda day: settlement_day(day, 3, 'friday'), dates), None)
	expired = True if date.day > 16 else False
	sec_code = contract_code(date.month)
	if exp_week is not None or expired:
		if product.lower() in ("fut", "futures"):
			if date.month % 3 == 0:
				sec_code = contract_code(date.month + 3)
			if date.month % 3 == 1:
				sec_code = contract_code(date.month + 2)
			if date.month % 3 == 2:
				sec_code = contract_code(date.month + 1)
		if product.lower() in ("opt", "options.py"):
			sec_code = contract_code(date.month + 1)
	sec_desc = instrument + sec_code + contract_year(str(date.year))
	return sec_desc


fixDate = ""


def __day_filter__(line):
	global fixDate
	filter_date = b'\x0152=' + str(fixDate).encode()
	if filter_date in line:
		return line


def __metrics__(line):
	# GET SECURITY ID
	sec = re.search(b'(\x0148\=)(.*)(\x01)', line)
	sec = sec.group(2).split(b'\x01')[0]
	# GET SECURITY DESCRIPTION
	secdes = re.search(b'(\x01107\=)(.*)(\x01)', line)
	secdes = secdes.group(2).split(b'\x01')[0]
	# GET SENDING DATE TAG 52
	day = line.split(b'\x0152=')[1].split(b'\x01')[0][0:8]
	return b','.join([sec, secdes, day])


SecurityID = None


def __secfilter__(line):
	global SecurityID
	sec_desc = b'\x0148=' + SecurityID.encode() + b'\x01' in line
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
