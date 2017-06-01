"""
Created on Fri Jul 22 17:33:13 2016
@author: jlroo
"""

import gzip
import bz2
import multiprocessing as mp
import re
import datetime
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


def contract_code(month, codes="F,G,H,J,K,M,N,Q,U,V,X,Z,F,G,H,J,K,M,N,Q,U,V,X,Z"):
	if codes == "":
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


class FixData:
	dates = []
	stats = {}
	contracts = {}

	def __init__(self, fixfile, src):
		self.data = fixfile
		self.path = src["path"]

		peek = self.data.peek().split(b"\n")[0]
		day0 = peek[peek.find(b'\x0152=') + 4:peek.find(b'\x0152=') + 12]

		if src["period"] == "weekly":
			start = datetime.datetime(year=int(day0[:4]), month=int(day0[4:6]), day=int(day0[6:8]))
			self.dates = [start + datetime.timedelta(days=i) for i in range(6)]
		else:
			raise ValueError("Supported time period: weekly data to get dates")

	"""
                       def securities

        This function returns the securities in the data
        by the expiration month

        returns a dictionary

        {MONTH: {SEC_ID:SEC_DESC}

    """

	def securities(self):
		months = set("F,G,H,J,K,M,N,Q,U,V,X,Z".split(","))
		for line in self.data:
			desc = line[line.find(b'd\x01'):line.find(b'd\x01') + 1]
			if desc != b'd':
				break
			sec_id = int(line.split(b'\x0148=')[1].split(b'\x01')[0])
			sec_desc = line.split(b'\x01107=')[1].split(b'\x01')[0].decode()
			sec_key = sec_desc[0:4]
			if sec_key not in self.contracts.keys():
				self.contracts[sec_key] = {"FUT": {}, "OPT": {}, "SPREAD": {}}
			for month in months:
				if month in sec_desc:
					if len(sec_desc) < 7:
						self.contracts[sec_key]['FUT'][sec_id] = sec_desc
					if 'P' in sec_desc or 'C' in sec_desc:
						self.contracts[sec_key]['OPT'][sec_id] = sec_desc
					if '-' in sec_desc:
						self.contracts[sec_key]['SPREAD'][sec_id] = sec_desc
		self.data.seek(0)
		return self.contracts

	"""
                       def data_metrics

        This function returns the number of messages
        sent in a particular date.

        returns a dictionary

        {DAY: VOLUME}
    """

	def data_metrics(self, chunksize=10 ** 4, file_out=False, path=""):
		desc = {}
		table = defaultdict(dict)
		with mp.Pool() as pool:
			data_map = pool.imap(__metrics__, self.data, chunksize)
			for entry in data_map:
				day = entry.split(b',')[2][0:8].decode()
				sec = entry.split(b',')[0].decode()
				secdesc = entry.split(b',')[1].decode()
				desc[sec] = secdesc
				if sec not in table[day].keys():
					table[day][sec] = 1
				else:
					table[day][sec] += 1
		if file_out is False:
			fix_stats = defaultdict(dict)
			for day in sorted(table.keys()):
				fix_stats[day] = defaultdict(dict)
				for sec in table[day]:
					fix_stats[day][sec]["desc"] = desc[sec]
					fix_stats[day][sec]["vol"] = table[day][sec]
			return fix_stats
		else:
			header = b'SecurityID,SecurityDesc,Volume,SendingDate' + b'\n'
			for day in sorted(table.keys()):
				with open(path + "stats_" + day + ".csv", "wb") as f:
					f.write(header)
					for sec in table[day]:
						f.write(b','.join(
							[sec.encode(), desc[sec].encode(), str(table[day][sec]).encode(), day.encode()]) + b'\n')
		self.data.seek(0)

	"""
                        def split_by

        The week to day function take a path to the fix file
        and a list with days corresponding to the trading of
        that week and breaks the Fix week file into its
        associate trading days.

        This functions creates a new gzip file located in
        the same path as the weekly data.

    """

	def split_by(self, dates, chunksize=10 ** 4, file_out=False):
		for day in dates:
			global fixDate
			fixDate = str(day).encode()
			path_out = self.path[:-4] + "_" + str(day) + ".bz2"
			with mp.Pool() as pool:
				msg_day = pool.imap(__day_filter__, self.data, chunksize)
				if file_out is True:
					with bz2.open(path_out, 'ab') as f:
						for entry in msg_day:
							f.write(entry)
				else:
					for entry in msg_day:
						return entry
			self.data.seek(0)

	"""
                        def filter_by

        This function takes a path to a fix file and
        a security id in order to create a new list or
        fix file with messages from that security.

    """

	def filter_by(self, security_id, file_out=False):
		sec_id = b"\x0148=" + security_id.encode() + b"\x01"
		tag = lambda e: True if sec_id in e else False
		if file_out is False:
			sec = []
			for line in iter(filter(tag, self.data)):
				header = line.split(b'\x01279')[0]
				msg_type = line[line.find(b'35=') + 3:line.find(b'35=') + 4]
				if b'X' == msg_type:
					body = line.split(b'\x0110=')[0]
					body = body.split(b'\x01279')[1:]
					body = [b'\x01279' + entry for entry in body]
					end = b'\x0110' + line.split(b'\x0110')[-1]
					for entry in body:
						if sec_id in entry:
							header += entry
						else:
							pass
					msg = header + end
					sec.append(msg)
				else:
					sec.append(line)
			return sec
		else:
			print("Using default compression bzip")
			path_out = self.path[:-4] + "_ID" + str(security_id) + ".bz2"
			with bz2.open(path_out, 'wb') as fix_sec:
				filtered = self.filter_by(security_id, file_out=False)
				fix_sec.writelines(filtered)
		self.data.seek(0)
"""
	def contracts_filter(self, contracts, chunksize=10 ** 4):
		global all_contracts
		all_contracts = contracts
		with mp.Pool() as pool:
			filtered = pool.map(__secfilter__, self.data, chunksize)
		return filtered
"""