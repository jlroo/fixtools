# -*- coding: utf-8 -*-

import datetime as __datetime__
import pandas as __pd__
import numpy as __np__
import multiprocessing as __mp__
from collections import defaultdict
from fixtools.util.util import expiration_date,  open_fix
from fixtools.io.fixfast import FixDict


def options_table(path=None,
                  files=None,
                  filename=None,
                  num_orders=1,
                  chunksize=32000,
                  path_out=None,
                  return_table=True):
    
    if path[-1] != "/":
        path = path + "/"
        
    if files:
        dfs = []
        for filename in files:
            fpath = path + filename
            fixdata = open_fix(fpath, compression=False)
            fix_dict = FixDict(num_orders)
            with __mp__.Pool() as pool:
                df = pool.map(fix_dict.to_dict, fixdata.data, chunksize=chunksize)
            dfs.append(__pd__.DataFrame.from_dict(df))
        options = __pd__.concat(dfs)

    elif filename:
        fpath = path + filename
        fixdata = open_fix(fpath, compression=False)
        fix_dict = FixDict(num_orders)
        with __mp__.Pool() as pool:
            df = pool.map(fix_dict.to_dict, fixdata.data, chunksize=chunksize)
        options = __pd__.DataFrame.from_dict(df)
    
    options = options.replace('NA',  __np__.nan)
    
    if path_out:
        if path_out[-1] != "/":
            path_out = path_out + "/"
        fname = path_out + filename[:-5] + "OPTIONS.csv"
        options.to_csv(fname,  index=False)
    
    if return_table:
        return options


def futures_table(path=None,
                  filename=None,
                  num_orders=1,
                  chunksize=32000,
                  path_out=None,
                  return_table=True):

    if path[-1] != "/":
        path = path + "/"

    fpath = path + filename
    fixdata = open_fix(fpath, compression=False)
    fix_dict = FixDict(num_orders)
    with __mp__.Pool() as pool:
        futures = pool.map(fix_dict.to_dict, fixdata.data, chunksize=chunksize)
    futures = __pd__.DataFrame.from_dict(futures)
    futures = futures.replace('NA',  __np__.nan)

    if path_out:
        if path_out[-1] != "/":
            path_out = path_out + "/"
        fname = path_out + filename + ".csv"
        futures.to_csv(fname,  index=False)

    if return_table:
        return futures


def __timemap__(item):
    sending_time = item[9]
    date = __datetime__.datetime.strptime(str(sending_time), "%Y%m%d%H%M%S%f")
    ymd = int(str(date)[0:10].replace("-",  ""))
    return ymd, date.hour, sending_time


def time_table(futures, options, chunksize=32000):
    with __mp__.Pool() as pool:
        fut_times = pool.map(__timemap__, futures.as_matrix(), chunksize=chunksize)
    grouped = {"futures": {}, "options": {}}
    for item in fut_times:
        ymd = item[0]
        if ymd not in grouped["futures"].keys():
            grouped["futures"][ymd] = defaultdict(list)
            grouped["futures"][ymd][item[1]].append(item[2])
        else:
            grouped["futures"][ymd][item[1]].append(item[2])
    with __mp__.Pool() as pool:
        opt_times = pool.map(__timemap__, options.as_matrix())
    for item in opt_times:
        ymd = item[0]
        if ymd not in grouped["options"].keys():
            grouped["options"][ymd] = defaultdict(list)
            grouped["options"][ymd][item[1]].append(item[2])
        else:
            grouped["options"][ymd][item[1]].append(item[2])
    return grouped


def search_out(result, 
               timestamp, 
               path_out, 
               ordered = ['share_strike','put_call',
                          'share_pv_strike','put_call_diff',
                          'strike_price','trade_date',
                          'exp_date', 'exp_days','fut_bid_price', 
                          'opt_p_bid_price', 'opt_c_bid_price',
                          'fut_offer_price', 'opt_p_offer_price', 
                          'opt_c_offer_price','fut_msg_seq_num', 
                          'opt_p_msg_seq_num', 'opt_c_msg_seq_num',
                          'fut_sending_time', 'opt_p_sending_time', 
                          'opt_c_sending_time', 'fut_bid_size',
                          'opt_p_bid_size', 'opt_c_bid_size', 
                          'fut_offer_size', 'opt_p_offer_size', 
                          'opt_c_offer_size', 'fut_bid_level',
                          'opt_p_bid_level', 'opt_p_offer_level',
                          'fut_offer_level', 'opt_c_bid_level',
                          'opt_c_offer_level', 'fut_sec_id', 
                          'opt_p_sec_id', 'opt_c_sec_id', 
                          'fut_sec_desc', 'opt_p_desc', 
                          'opt_c_desc']):
    if path_out[-1] != "/":
        path_out = path_out + "/"
    fname = path_out + str(timestamp) + ".csv"
    df = []
    for k in result.keys():
        df.append(__pd__.DataFrame.from_dict(result[k],  orient='index'))
    df = __pd__.concat(df)
    df.reset_index()
    df['opt_p_sending_time'] = [str(i) if str(i) != 'nan' else i for i in df['opt_p_sending_time']]
    df['opt_c_sending_time'] = [str(i) if str(i) != 'nan' else i for i in df['opt_c_sending_time']]
    df['fut_sending_time'] = [str(i) if str(i) != 'nan' else i for i in df['fut_sending_time']]
    if ordered:
        df = df[ordered]
    df.to_csv(fname, index=False, quotechar='"')


def __putcall__(item, codes):
    sec_desc = str(item['security_desc'])
    strike_price = int(sec_desc.split(" ")[1][1:])
    order_type = sec_desc.split(" ")[1][0]
    trade_day = str(item['trade_date'])
    year = int(trade_day[0:4])
    month = int(trade_day[4:6])
    day = int(trade_day[6:])
    trade_date = __datetime__.datetime(year,  month,  day)
    month_exp = codes[sec_desc[2].lower()]
    if month == 12:
        year += 1
    exp_date = expiration_date(year,  month_exp,  3,  day='friday')
    delta = exp_date - trade_date
    if order_type == "C":
        dd = {"strike_price": strike_price, "trade_date": trade_date, "exp_date": exp_date, "exp_days": delta.days,
              "opt_c_sec_id": item['security_id'], "opt_c_desc":sec_desc, "opt_c_msg_seq_num": item['msg_seq_num'],
              "opt_c_sending_time": str(item['sending_time']), "opt_c_bid_price": item['bid_price'],
              "opt_c_bid_size": item['bid_size'], "opt_c_bid_level": item['bid_level'],
              "opt_c_offer_price": item['offer_price'], "opt_c_offer_size": item['offer_size'],
              "opt_c_offer_level": item['offer_level']}
    else:
        dd = {"strike_price": strike_price, "trade_date": trade_date, "exp_date": exp_date, "exp_days": delta.days,
              "opt_p_sec_id": item['security_id'], "opt_p_desc": sec_desc, "opt_p_msg_seq_num": item['msg_seq_num'],
              "opt_p_sending_time": str(item['sending_time']), "opt_p_bid_price": item['bid_price'],
              "opt_p_bid_size": item['bid_size'], "opt_p_bid_level": item['bid_level'],
              "opt_p_offer_price": item['offer_price'], "opt_p_offer_size": item['offer_size'],
              "opt_p_offer_level": item['offer_level']}
    return dd


def put_call_table(item, codes):
    sec_desc = str(item['security_desc'])
    trade_day = str(item['trade_date'])
    year, month, day = int(trade_day[0:4]), int(trade_day[4:6]), int(trade_day[6:])
    trade_date = __datetime__.datetime(year, month, day)
    month_exp = codes[sec_desc[2].lower()]
    if month == 12:
        year = year + 1
    exp_date = expiration_date(year, month_exp, 3, day='friday')
    delta = exp_date - trade_date
    dd = {"trade_date": trade_date, "exp_days": delta.days,
          "fut_sec_id": item['security_id'], "fut_sec_desc": item['security_desc'],
          "fut_msg_seq_num": item['msg_seq_num'], "fut_sending_time": str(item['sending_time']),
          "fut_bid_price": item['bid_price'], "fut_bid_size": item['bid_size'],
          "fut_bid_level": item['bid_level'], "fut_offer_price": item['offer_price'],
          "fut_offer_size": item['offer_size'], "fut_offer_level": item['offer_level']}
    columns = ["opt_p_sec_id", "opt_p_desc", "opt_p_msg_seq_num", "opt_p_sending_time",
               "opt_p_bid_price", "opt_p_bid_size", "opt_p_bid_level", "opt_p_offer_price",
               "opt_p_offer_size", "opt_p_offer_level", "opt_c_sec_id", "opt_c_desc",
               "opt_c_msg_seq_num", "opt_c_sending_time", "opt_c_bid_price", "opt_c_bid_size",
               "opt_c_bid_level", "opt_c_offer_price", "opt_c_offer_size", "opt_c_offer_level"]
    for col in columns:
        dd[col] = __np__.nan
    return dd


def put_call_query(futures, options, timestamp,
                   month_codes=None, level_limit=1):
    if month_codes is None:
        month_codes = "F,G,H,J,K,M,N,Q,U,V,X,Z"
    month_codes = month_codes.lower()
    table = {"fut": []}
    codes = {k[1]: k[0] for k in enumerate(month_codes.rsplit(","), 1)}
    query = futures[(futures['bid_level'] <= level_limit)]
    query = query[__pd__.notnull(query['security_desc'])]
    query = query[query['sending_time'] <= int(timestamp)]
    query = query.sort_values('msg_seq_num')
    query = query.reset_index()
    del query['index']
    fut_dict = query.tail(level_limit).to_dict(orient="records")
    for item in fut_dict:
        dd = put_call_table(item, codes)
        table["fut"].append(dd.copy())
    query = options[(options['bid_level'] <= level_limit)]
    query = query[__pd__.notnull(query['security_desc'])]
    query = query[query['sending_time'] <= int(timestamp)]
    query = query.sort_values('msg_seq_num')
    query = query.reset_index()
    del query['index']
    opts = list(query['security_id'].unique())
    for sec in opts:
        sec_query = query[query['security_id'] == sec]
        item = sec_query.tail(level_limit).to_dict(orient="records")[-1]
        sec_desc = str(item['security_desc'])
        price = int(sec_desc.split(" ")[1][1:])
        if price not in table.keys():
            table[price] = {i: {} for i in range(level_limit)}
            dd = __putcall__(item, codes)
            table_dd = table["fut"][level_limit - 1].copy()
            table_dd.update(dd)
            table[price][level_limit - 1] = table_dd.copy()
        else:
            dd = __putcall__(item, codes)
            table[price][level_limit - 1].update(dd)
    del table["fut"]
    return table


def put_call_parity(futures, options, 
                    rates_table, timestamp, 
                    month_codes=None, level_limit=1):
    table = put_call_query(futures, options, 
                           timestamp, month_codes, 
                           level_limit=level_limit)
    rate_dict = {}
    rates = rates_table.to_dict(orient='list')
    date = __datetime__.datetime(int(timestamp[0:4]), int(timestamp[4:6]), int(timestamp[6:8]))
    for i, day in enumerate(rates[list(rates.keys())[0]]):
        day_time = __datetime__.datetime(int(day[0:4]), int(day[5:7]), int(day[8:10]))
        rate_dict[day_time] = rates[list(rates.keys())[1]][i]
    if date not in rate_dict.keys():
        date = date + __datetime__.timedelta(days=1)
    risk_rate = rate_dict[date]
    for k in table.keys():
        exp_days = table[k][0]['exp_days']
        fut_bid_price = int(table[k][0]['fut_bid_price'])/100
        fut_offer_price = int(table[k][0]['fut_offer_price'])/100
        fut_price = (fut_bid_price + fut_offer_price)/2
        put_bid_price = int(table[k][0]['opt_p_bid_price'])/100
        put_offer_price = int(table[k][0]['opt_p_offer_price'])/100
        put_price = (put_bid_price + put_offer_price)/2
        call_bid_price = int(table[k][0]['opt_c_bid_price'])/100
        call_offer_price = int(table[k][0]['opt_c_offer_price'])/100
        call_price = (call_bid_price + call_offer_price)/2
        share_strike = fut_price * __np__.exp(-risk_rate*(exp_days/365)) 
        share_pv_strike = share_strike - (k * __np__.exp(-risk_rate*(exp_days/365)))
        share_strike = share_strike - k
        put_call = call_price - put_price
        if put_call > share_pv_strike:
            diff = put_call - share_pv_strike
        elif put_call < share_strike:
            diff = put_call - share_strike
        else:
            diff = 0
        table[k][0]['share_strike'] = share_strike
        table[k][0]['share_pv_strike'] = share_pv_strike
        table[k][0]['put_call'] = put_call
        table[k][0]['put_call_diff'] = diff
    return table
