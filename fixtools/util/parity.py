# -*- coding: utf-8 -*-

import datetime as __datetime__
import pandas as __pd__
import numpy as __np__
from fixtools.util.util import expiration_date, open_fix
from fixtools.io.fixfast import to_dict


def options_table(path = None,
                  files=None,
                  filename=None,
                  top_order=1,
                  write_csv = True,
                  path_out = None,
                  return_table = True):

    if path[-1] != "/":
        path = path + "/"
    if files:
        dfs = []
        for filename in files:
            fpath = path + filename
            fixdata = open_fix(fpath, compression=False)
            df = [to_dict(i, top_order = top_order) for i in fixdata.data]
            dfs.append(__pd__.DataFrame.from_dict(df))
        options = __pd__.concat(dfs)
    elif filename:
        fpath = path + filename
        fixdata = open_fix(fpath, compression=False)
        df = [to_dict(i, top_order = top_order) for i in fixdata.data]
        options = __pd__.DataFrame.from_dict(df)
    options = options.replace('NA',__np__.nan)
    options.reset_index(level=0)
    if write_csv:
        if path_out[-1] != "/":
            path_out = path_out + "/"
        fname = path_out + filename[:-5] + "OPTIONS.csv"
        options.to_csv(fname, index=False)
    if return_table:
        return options


def futures_table(path = None,
                  filename = None,
                  top_order = 1,
                  write_csv = True,
                  path_out = None,
                  return_table = True):

    if path[-1] != "/":
        path = path + "/"
    fpath = path + filename
    fixdata = open_fix(fpath, compression=False)
    futures = [to_dict(i, top_order = top_order) for i in fixdata.data]
    futures = __pd__.DataFrame.from_dict(futures)
    futures = futures.replace('NA',__np__.nan)
    futures.reset_index(level=0)
    if write_csv:
        if path_out[-1] != "/":
            path_out = path_out + "/"
        fname = path_out + filename + ".csv"
        futures.to_csv(fname, index=False)
    if return_table:
        return futures


def time_table(futures, options):
    grouped = {"futures":{}, "options":{}}
    fut_times = set(futures['sending_time'])
    for t in fut_times:
        date = __datetime__.datetime.strptime(str(t),"%Y%m%d%H%M%S%f")
        ymd = int(str(date)[0:10].replace("-",""))
        if ymd not in grouped["futures"].keys():
            grouped["futures"][ymd] = {}
            grouped["futures"][ymd][date.hour] = []
            grouped["futures"][ymd][date.hour].append(t)
        else:
            if date.hour not in grouped["futures"][ymd].keys():
                grouped["futures"][ymd][date.hour] = []
                grouped["futures"][ymd][date.hour].append(t)
            else:
                grouped["futures"][ymd][date.hour].append(t)

    opt_times = set(options['sending_time'])
    for t in opt_times:
        date = __datetime__.datetime.strptime(str(t),"%Y%m%d%H%M%S%f")
        ymd = int(str(date)[0:10].replace("-",""))
        if ymd not in grouped["options"].keys():
            grouped["options"][ymd] = {}
            grouped["options"][ymd][date.hour] = []
            grouped["options"][ymd][date.hour].append(t)
        else:
            if date.hour not in grouped["options"][ymd].keys():
                grouped["options"][ymd][date.hour] = []
                grouped["options"][ymd][date.hour].append(t)
            else:
                grouped["options"][ymd][date.hour].append(t)
    return grouped


def search_out(result, timestamp, path, string_time = True):
    if path[-1] != "/":
        path = path + "/"
    fname = path + str(timestamp) + ".csv"
    df = []
    for k in result.keys():
        df.append(__pd__.DataFrame.from_dict(result[k], orient='index'))
    df = __pd__.concat(df)
    df.reset_index(level=0)
    if string_time:
        time_labels = [i for i in df.columns if "time" in i]
        for label in time_labels:
            df[label] = [str(i) for i in list(df[label])]
    df.to_csv(fname, index=False)


def __put_call__(item,codes):
    dd = {}
    sec_desc = str(item['security_desc'])
    strike_price = int(sec_desc.split(" ")[1][1:])
    order_type = sec_desc.split(" ")[1][0]
    trade_day = str(item['trade_date'])
    year = int(trade_day[0:4])
    month = int(trade_day[4:6])
    day = int(trade_day[6:])
    trade_date = __datetime__.datetime(year,month,day)
    month_exp = codes[sec_desc[2].lower()]
    if month == 12: year+=1
    exp_date = expiration_date(year,month_exp,3,day='friday')
    dd['strike_price'] = strike_price
    dd['trade_date'] = trade_date
    dd['exp_date'] = exp_date
    delta = exp_date - trade_date
    dd['exp_days'] = delta.days
    if order_type == "C":
        dd["opt_c_sec_id"] = item['security_id']
        dd["opt_c_desc"] = sec_desc
        dd["opt_c_msg_seq_num"] = item['msg_seq_num']
        dd["opt_c_sending_time"] = str(item['sending_time'])
        dd["opt_c_bid_price"] = item['bid_price']
        dd["opt_c_bid_size"] = item['bid_size']
        dd["opt_c_bid_level"] = item['bid_level']
        dd["opt_c_offer_price"] = item['offer_price']
        dd["opt_c_offer_size"] = item['offer_size']
        dd["opt_c_offer_level"] = item['offer_level']
        dd["opt_p_sec_id"] = __np__.nan
        dd["opt_p_desc"] = __np__.nan
        dd["opt_p_msg_seq_num"] = __np__.nan
        dd["opt_p_sending_time"] = __np__.nan
        dd["opt_p_bid_price"] = __np__.nan
        dd["opt_p_bid_size"] = __np__.nan
        dd["opt_p_bid_level"] = __np__.nan
        dd["opt_p_offer_price"] = __np__.nan
        dd["opt_p_offer_size"] = __np__.nan
        dd["opt_p_offer_level"] = __np__.nan
    else:
        dd["opt_c_sec_id"] = __np__.nan
        dd["opt_c_desc"] = __np__.nan
        dd["opt_c_msg_seq_num"] = __np__.nan
        dd["opt_c_sending_time"] = __np__.nan
        dd["opt_c_bid_price"] = __np__.nan
        dd["opt_c_bid_size"] = __np__.nan
        dd["opt_c_bid_level"] = __np__.nan
        dd["opt_c_offer_price"] = __np__.nan
        dd["opt_c_offer_size"] = __np__.nan
        dd["opt_c_offer_level"] = __np__.nan
        dd["opt_p_sec_id"] = item['security_id']
        dd["opt_p_desc"] = sec_desc
        dd["opt_p_msg_seq_num"] = item['msg_seq_num']
        dd["opt_p_sending_time"] = str(item['sending_time'])
        dd["opt_p_bid_price"] = item['bid_price']
        dd["opt_p_bid_size"] = item['bid_size']
        dd["opt_p_bid_level"] = item['bid_level']
        dd["opt_p_offer_price"] = item['offer_price']
        dd["opt_p_offer_size"] = item['offer_size']
        dd["opt_p_offer_level"] = item['offer_level']
    return dd


def put_call_query(futures = None,
                   options = None,
                   timestamp = None,
                   futures_time = None,
                   options_time = None,
                   month_codes = "F,G,H,J,K,M,N,Q,U,V,X,Z",
                   level_limit = 1,
                   time_format = "%Y%m%d%H%M%S%f",
                   milliseconds = 1000):
    if not month_codes:
        month_codes = "F,G,H,J,K,M,N,Q,U,V,X,Z"
    month_codes = month_codes.lower()
    table = {"fut":[]}
    dd = {}
    codes = {k[1]: k[0] for k in enumerate(month_codes.rsplit(","), 1)}
    futures = futures.sort_values('sending_time')
    query = futures[(futures['bid_level']<=level_limit)]
    query = query[__pd__.notnull(query['security_desc'])]
    query = query[query['sending_time']<int(timestamp)]
    query = query.sort_values('sending_time')
    query = query.reset_index()
    fut_dict = query.tail(level_limit).to_dict(orient = "index")
    for k in fut_dict.keys():
        strike_price = 0
        sec_desc = str(fut_dict[k]['security_desc'])
        trade_day = str(fut_dict[k]['trade_date'])
        year,month,day = int(trade_day[0:4]),int(trade_day[4:6]),int(trade_day[6:])
        trade_date = __datetime__.datetime(year,month,day)
        month_exp = codes[sec_desc[2].lower()]
        if month ==12:
            year = year+1
        exp_date = expiration_date(year,month_exp,3,day='friday')
        dd["trade_date"] = trade_date
        dd["fut_exp_date"] = exp_date
        delta = exp_date - trade_date
        dd['exp_days'] = delta.days
        dd["fut_sec_id"] = fut_dict[k]['security_id']
        dd["fut_sec_desc"] = fut_dict[k]['security_desc']
        dd["fut_msg_seq_num"] = fut_dict[k]['msg_seq_num']
        dd["fut_sending_time"] = str(fut_dict[k]['sending_time'])
        dd["fut_bid_price"] = fut_dict[k]['bid_price']
        dd["fut_bid_size"] = fut_dict[k]['bid_size']
        dd["fut_bid_level"] = fut_dict[k]['bid_level']
        dd["fut_offer_price"] = fut_dict[k]['offer_price']
        dd["fut_offer_size"] = fut_dict[k]['offer_size']
        dd["fut_offer_level"] = fut_dict[k]['offer_level']
        table["fut"].append(dd.copy())

    options = options[(options['bid_level']<=level_limit)]
    query = options[__pd__.notnull(options['security_desc'])]
    query = query.sort_values('sending_time')
    query = query[query['sending_time']<int(timestamp)]
    query = query.reset_index()
    opts = list(query['security_id'].unique())

    for sec in opts:
        sec_query = query[query['security_id']==sec]
        sec_query = sec_query.tail(level_limit)
        optdict = sec_query.to_dict(orient = "index")
        key = list(optdict.keys())[0]
        item = optdict[key]
        sec_desc = str(item['security_desc'])
        strike_price = int(sec_desc.split(" ")[1][1:])
        dd = {}
        if strike_price not in table.keys():
            table[strike_price] = {i:{} for i in range(level_limit)}
            dd = __put_call__(item, codes)
            dd.update(table["fut"][level_limit-1])
            table[strike_price][level_limit-1] = dd.copy()
        else:
            dd = __put_call__(item, codes)
            dd.update(table[strike_price][level_limit-1])
            table[strike_price][level_limit-1] = dd.copy()
    del table["fut"]
    return table


