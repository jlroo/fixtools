"""
Created on Fri Jul 22 17:33:13 2016
@author: jlroo

"""

import multiprocessing as __mp__
from collections import defaultdict as __defaultdict__
import datetime as __datetime__
import bz2 as __bz2__
import re as __re__
from dateutil import tz as __tz__
import os as __os__


__fixDate__ = None


def from_time(timestamp,
              stamp_date,
              stamp_time,
              time_format='%Y%m%d%H%M%S%f',
              from_zone='UTC',
              to_zone='America/Chicago'):

    if not timestamp:
        timestamp = stamp_date + stamp_time
    from_zone = __tz__.gettz(from_zone)
    to_zone = __tz__.gettz(to_zone)
    stamp = __datetime__.datetime.strptime(timestamp, time_format)
    stamp = stamp.replace(tzinfo=from_zone)
    stamp = stamp.astimezone(to_zone)
    date = stamp.strftime("%Y%m%d")
    time = stamp.strftime("%H%M%S%f")[:-3]
    return {"date": date, "time": time, "timestamp": date+time}


class DayFilter:
    def __init__(self, fix_date):
        self.__fixDate__ = fix_date

    def filter(self, line):
        filter_date = b'\x0152=' + str(self.__fixDate__).encode()
        if filter_date in line:
            return line


def __metrics__(line):
    # GET SECURITY ID
    sec = __re__.search(b'(\x0148\=)(.*)(\x01)', line)
    sec = sec.group(2).split(b'\x01')[0]
    # GET SECURITY DESCRIPTION
    secdes = __re__.search(b'(\x01107\=)(.*)(\x01)', line)
    secdes = secdes.group(2).split(b'\x01')[0]
    # GET SENDING DATE TAG 52
    day = line.split(b'\x0152=')[1].split(b'\x01')[0][0:8]
    return b','.join([sec, secdes, day])


def __secdesc__(data, group="ES", group_code="EZ", max_lines=10000):
    cnt = 0
    lines = []
    code = (group.encode(), group_code.encode())
    for line in data:
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
            lines.append({sec_id: sec_desc})
        cnt += 1
    data.seek(0)
    return lines


def files_tree(path):
    files_list = list(__os__.walk(path))[0][2]
    files = __defaultdict__(dict)
    for f in files_list:
        key = int(f.split("-")[0])
        if key not in files.keys():
            files[key] = {"options": [], "futures":[]}
        if "C" in f or "P" in f:
            files[key]["options"].append(f)
        else:
            files[key]["futures"].append(f)
    return files


def to_csv(line, top_order=3):
    for item in [line]:
        tag34 = item.split(b"\x0134=")[1].split(b"\x01")[0]
        tag48 = item.split(b"\x0148=")[1].split(b"\x01")[0]
        tag52 = item.split(b"\x0152=")[1].split(b"\x01")[0]
        tag75 = item.split(b"\x0175=")[1].split(b"\x01")[0]
        tag107 = item.split(b"\x01107=")[1].split(b"\x01")[0]
        body = item.split(b'\x0110=')[0].split(b'\x01279')[1:]
        bids = body[:len(body) // 2]
        offers = body[len(body) // 2:]
        row = []
        for i in range(top_order):
            tag270b = bids[i].split(b'\x01270=')[1].split(b'\x01')[0]
            tag270s = offers[i].split(b'\x01270=')[1].split(b'\x01')[0]
            tag271b = bids[i].split(b'\x01271=')[1].split(b'\x01')[0]
            tag271s = offers[i].split(b'\x01271=')[1].split(b'\x01')[0]
            row.append(b",".join([tag48, tag107, tag34, tag52, tag75, tag270b,
                                  tag271b, str(1+i).encode(),
                                  tag270s, tag271s, str(1+i).encode()]) + b"\n")
        return b" ".join([e for e in row])


class FixDict:

    def __init__(self, num_orders):
        self.num_orders = num_orders

    def to_dict(self, line):
        dd = {}
        for item in [line]:
            dd["msg_seq_num"] = int(item.split(b"\x0134=")[1].split(b"\x01")[0].decode())
            dd["security_id"] = item.split(b"\x0148=")[1].split(b"\x01")[0].decode()
            dd["sending_time"] = int(item.split(b"\x0152=")[1].split(b"\x01")[0].decode())
            dd["trade_date"] = item.split(b"\x0175=")[1].split(b"\x01")[0].decode()
            dd["security_desc"] = item.split(b"\x01107=")[1].split(b"\x01")[0].decode()
            body = item.split(b'\x0110=')[0].split(b'\x01279')[1:]
            bids = body[:len(body) // 2]
            offers = body[len(body) // 2:]
            if self.num_orders > 1:
                dd["bid_price"] = []
                dd["bid_size"] = []
                dd["bid_level"] = []
                dd["offer_price"] = []
                dd["offer_size"] = []
                dd["offer_level"] = []
                for i in range(self.num_orders):
                    dd["bid_price"].append(bids[i].split(b'\x01270=')[1].split(b'\x01')[0].decode())
                    dd["bid_size"].append(bids[i].split(b'\x01271=')[1].split(b'\x01')[0].decode())
                    dd["bid_level"].append(i + 1)
                    dd["offer_price"].append(offers[i].split(b'\x01270=')[1].split(b'\x01')[0].decode())
                    dd["offer_size"].append(offers[i].split(b'\x01271=')[1].split(b'\x01')[0].decode())
                    dd["offer_level"].append(i + 1)
            else:
                dd["bid_price"] = bids[0].split(b'\x01270=')[1].split(b'\x01')[0].decode()
                dd["bid_size"] = bids[0].split(b'\x01271=')[1].split(b'\x01')[0].decode()
                dd["bid_level"] = self.num_orders
                dd["offer_price"] = offers[0].split(b'\x01270=')[1].split(b'\x01')[0].decode()
                dd["offer_size"] = offers[0].split(b'\x01271=')[1].split(b'\x01')[0].decode()
                dd["offer_level"] = self.num_orders
            return dd


class FixData:
    dates = []
    stats = {}
    contracts = {}

    def __init__(self, fixfile, src):
        self.data = fixfile
        self.path = src["path"]
        if b'\x0152=' in self.data.readline():
            peek = self.data.readline().split(b"\n")[0]
            day0 = peek[peek.find(b'\x0152=') + 4:peek.find(b'\x0152=') + 12]
            if src["period"] == "weekly":
                start = __datetime__.datetime(year=int(day0[:4]), month=int(day0[4:6]), day=int(day0[6:8]))
                self.dates = [start + __datetime__.timedelta(days=i) for i in range(6)]
            else:
                raise ValueError("Supported time period: weekly data to get dates")

    # 			   def securities
    #
    # This function returns the securities in the data
    # by the expiration month returns a dictionary
    #
    # {MONTH: {SEC_ID:SEC_DESC}
    #
    #

    def securities( self , group="ES" , group_code="EZ" , max_lines=100000 ):
        months = set("F,G,H,J,K,M,N,Q,U,V,X,Z".split(","))
        securities = __secdesc__(self.data, group, group_code, max_lines)
        filtered = {list(item.keys())[0]: list(item.values())[0] for item in securities}
        for sec_id in filtered.keys():
            sec_desc = filtered[sec_id]
            if len(sec_desc) < 12:
                sec_key = sec_desc[0:4]
                if sec_key not in self.contracts.keys():
                    self.contracts[sec_key] = {"FUT": {}, "OPT": {}, "PAIRS": {}, "SPREAD": {}}
                for month in months:
                    if month in sec_desc:
                        if len(sec_desc) < 7:
                            self.contracts[sec_key]['FUT'][sec_id] = sec_desc
                        if 'P' in sec_desc or 'C' in sec_desc:
                            self.contracts[sec_key]['OPT'][sec_id] = sec_desc
                            if 'C' in sec_desc:
                                call_price = int(sec_desc.split(" C")[-1])
                                if call_price not in self.contracts[sec_key]['PAIRS'].keys():
                                    self.contracts[sec_key]['PAIRS'][call_price] = {}
                                    self.contracts[sec_key]['PAIRS'][call_price][sec_id] = sec_desc
                                else:
                                    self.contracts[sec_key]['PAIRS'][call_price][sec_id] = sec_desc
                            if "P" in sec_desc:
                                put_price = int(sec_desc.split(" P")[-1])
                                if put_price not in self.contracts[sec_key]['PAIRS'].keys():
                                    self.contracts[sec_key]['PAIRS'][put_price] = {}
                                    self.contracts[sec_key]['PAIRS'][put_price][sec_id] = sec_desc
                                else:
                                    self.contracts[sec_key]['PAIRS'][put_price][sec_id] = sec_desc
                        if '-' in sec_desc:
                            self.contracts[sec_key]['SPREAD'][sec_id] = sec_desc

        for sec_key in self.contracts.keys():
            pairs = self.contracts[sec_key]['PAIRS'].copy()
            for price in pairs.keys():
                if len(pairs[price]) != 2:
                    del self.contracts[sec_key]['PAIRS'][price]

        self.data.seek(0)
        return self.contracts

    #
    # 			   def data_metrics
    #
    # This function returns the number of messages
    # sent in a particular date.
    #
    # returns a dictionary
    #
    # {DAY: VOLUME}

    def data_metrics(self, file_out=False, path="", chunksize=10 ** 4):
        desc = {}
        table = __defaultdict__(dict)
        with __mp__.Pool() as pool:
            data_map = pool.imap(__metrics__, self.data, chunksize)
            for entry in data_map:
                day = entry.split(b',')[2][0:8].decode()
                sec = entry.split(b',')[0].decode()
                sec_desc = entry.split(b',')[1].decode()
                desc[sec] = sec_desc
                if sec not in table[day].keys():
                    table[day][sec] = 1
                else:
                    table[day][sec] += 1
        if file_out is False:
            fix_stats = __defaultdict__(dict)
            for day in sorted(table.keys()):
                fix_stats[day] = __defaultdict__(dict)
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

    #
    # 				def split_by
    #
    # The week to day function take a path to the fix file
    # and a list with days corresponding to the trading of
    # that week and breaks the Fix week file into its
    # associate trading days.
    #
    # This functions creates a new gzip file located in
    # the same path as the weekly data.
    #

    def split_by(self, dates, chunksize=10 ** 4, file_out=False):
        for day in dates:
            path_out = self.path[:-4] + "_" + str(day) + ".bz2"
            day_filter = DayFilter(str(day).encode())
            with __mp__.Pool() as pool:
                msg_day = pool.imap(day_filter.filter, self.data, chunksize)
                if file_out is True:
                    with __bz2__.open(path_out, 'ab') as f:
                        for entry in msg_day:
                            f.write(entry)
                else:
                    for entry in msg_day:
                        return entry
            self.data.seek(0)

    # 				def filter_by
    #
    # This function takes a path to a fix file and
    # a security id in order to create a new list or
    # fix file with messages from that security.
    #

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
            with __bz2__.open(path_out, 'wb') as fix_sec:
                filtered = self.filter_by(security_id, file_out=False)
                fix_sec.writelines(filtered)
        self.data.seek(0)
