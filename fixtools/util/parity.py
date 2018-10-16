# -*- coding: utf-8 -*-

import datetime as __datetime__
import numpy as __np__
from pandas import Timestamp
from fixtools.util.util import expiration_date
from fixtools.util.search import timetable


def __depth__( depth_func="min" , size=None ):
    if depth_func == "max":
        return max(size)
    elif depth_func == "min":
        return min(size)


def __orderdict__( item , codes ):
    sec_desc = str(item['security_desc'])
    strike_price = int(sec_desc.split(" ")[1][1:])
    order_type = sec_desc.split(" ")[1][0]
    trade_day = str(item['trade_date'])
    year = int(trade_day[0:4])
    month = int(trade_day[4:6])
    day = int(trade_day[6:])
    trade_date = __datetime__.datetime(year , month , day)
    month_exp = codes[sec_desc[2].lower()]
    if month == 12:
        year += 1
    exp_date = expiration_date(year , month_exp , 3 , day='friday')
    delta = exp_date - trade_date
    if "c" in order_type.lower():
        dd = {"strike_price": strike_price ,
              "trade_date": trade_date ,
              "exp_date": exp_date ,
              "exp_days": delta.days ,
              "opt_c_sec_id": item['security_id'] ,
              "opt_c_desc": sec_desc ,
              "opt_c_msg_seq_num": item['msg_seq_num'] ,
              "opt_c_sending_time": str(item['sending_time']) ,
              "opt_c_bid_price": item['bid_price'] ,
              "opt_c_bid_size": item['bid_size'] ,
              "opt_c_bid_level": item['bid_level'] ,
              "opt_c_offer_price": item['offer_price'] ,
              "opt_c_offer_size": item['offer_size'] ,
              "opt_c_offer_level": item['offer_level']}
    else:
        dd = {"strike_price": strike_price ,
              "trade_date": trade_date ,
              "exp_date": exp_date ,
              "exp_days": delta.days ,
              "opt_p_sec_id": item['security_id'] ,
              "opt_p_desc": sec_desc ,
              "opt_p_msg_seq_num": item['msg_seq_num'] ,
              "opt_p_sending_time": str(item['sending_time']) ,
              "opt_p_bid_price": item['bid_price'] ,
              "opt_p_bid_size": item['bid_size'] ,
              "opt_p_bid_level": item['bid_level'] ,
              "opt_p_offer_price": item['offer_price'] ,
              "opt_p_offer_size": item['offer_size'] ,
              "opt_p_offer_level": item['offer_level']}
    return dd


def __bookdict__( item , codes ):
    """
    Creates a dictionary from the FIX order book
    :param item:
    :param codes:
    :return: Return python dictionary with the time to expiration
    """
    sec_desc = str(item['security_desc'])
    trade_day = str(item['trade_date'])
    year , month , day = int(trade_day[0:4]) , int(trade_day[4:6]) , int(trade_day[6:])
    trade_date = __datetime__.datetime(year , month , day)
    month_exp = codes[sec_desc[2].lower()]
    if month == 12:
        year = year + 1
    exp_date = expiration_date(year , month_exp , 3 , day='friday')
    delta = exp_date - trade_date
    dd = {"trade_date": trade_date ,
          "exp_days": delta.days ,
          "fut_sec_id": item['security_id'] ,
          "fut_sec_desc": item['security_desc'] ,
          "fut_msg_seq_num": item['msg_seq_num'] ,
          "fut_sending_time": str(item['sending_time']) ,
          "fut_bid_price": item['bid_price'] ,
          "fut_bid_size": item['bid_size'] ,
          "fut_bid_level": item['bid_level'] ,
          "fut_offer_price": item['offer_price'] ,
          "fut_offer_size": item['offer_size'] ,
          "fut_offer_level": item['offer_level']}
    columns = ["opt_p_sec_id" , "opt_p_desc" ,
               "opt_p_msg_seq_num" , "opt_p_sending_time" ,
               "opt_p_bid_price" , "opt_p_bid_size" ,
               "opt_p_bid_level" , "opt_p_offer_price" ,
               "opt_p_offer_size" , "opt_p_offer_level" ,
               "opt_c_sec_id" , "opt_c_desc" ,
               "opt_c_msg_seq_num" , "opt_c_sending_time" ,
               "opt_c_bid_price" , "opt_c_bid_size" ,
               "opt_c_bid_level" , "opt_c_offer_price" ,
               "opt_c_offer_size" , "opt_c_offer_level"]
    for col in columns:
        dd[col] = __np__.nan
    return dd


def top_book( futures=None , options=None , timestamp=None , month_codes=None ):
    """
    Search pandas dataframe for specific timestamp
    :param futures: Order book dataframe for futures contracts
    :param options: Order book for all options contracts
    :param timestamp: Timestamp to search in the order books
    :param month_codes: The codes to corresponding months. CME default "F,G,H,J,K,M,N,Q,U,V,X,Z"
    :return: Dictionary with the result of the timestamp search
    """
    book_level = 1
    if month_codes is None:
        month_codes = "F,G,H,J,K,M,N,Q,U,V,X,Z"
    month_codes = month_codes.lower()
    table = {"fut": []}
    codes = {k[1]: k[0] for k in enumerate(month_codes.rsplit(",") , 1)}
    query = futures[__np__.where((futures['bid_level'] == book_level) & (futures['sending_time'] <= timestamp))]
    query = query[~__np__.isnan(query['bid_price'])]
    query = query[~__np__.isnan(query['offer_price'])]
    query.sort(order='sending_time')
    item = query[-1]
    fut_dict = {n: item[i] for i , n in enumerate(item.dtype.names)}
    for item in [fut_dict]:
        dd = __bookdict__(item , codes)
        table["fut"].append(dd.copy())
    query = options[__np__.where((options['bid_level'] == book_level) & (options['sending_time'] <= timestamp))]
    query = query[~__np__.isnan(query['bid_price'])]
    query = query[~__np__.isnan(query['offer_price'])]
    query.sort(order='sending_time')
    opts = __np__.unique(query['security_id'])
    for sec in opts:
        sec_query = query[__np__.where(query['security_id'] == sec)]
        sec_query.sort(order='sending_time')
        item = sec_query[-1]
        item = {n: item[i] for i , n in enumerate(item.dtype.names)}
        sec_desc = item['security_desc']
        price = int(sec_desc.split(" ")[1][1:])
        if price not in table.keys():
            table[price] = {i: {} for i in range(book_level)}
            dd = __orderdict__(item , codes)
            table_dd = table["fut"][book_level - 1].copy()
            table_dd.update(dd)
            table[price][book_level - 1] = table_dd.copy()
        else:
            dd = __orderdict__(item , codes)
            table[price][book_level - 1].update(dd)
    del table["fut"]
    timestamp = str(timestamp)
    timestamp = Timestamp(year=int(timestamp[0:4]) , month=int(timestamp[4:6]) , day=int(timestamp[6:8]) ,
                          hour=int(timestamp[8:10]) , minute=int(timestamp[10:12]) , second=int(timestamp[12:14]) ,
                          microsecond=int(timestamp[14:]) * 1000 , unit="ms").ceil("H")
    for key in table.keys():
        opt_p_sending_time = table[key][book_level - 1]['opt_p_sending_time']
        opt_p_sending_time = [str(i) if str(i) != 'nan' else i for i in [opt_p_sending_time]][0]
        table[key][book_level - 1]['opt_p_sending_time'] = opt_p_sending_time
        opt_c_sending_time = table[key][book_level - 1]['opt_c_sending_time']
        opt_c_sending_time = [str(i) if str(i) != 'nan' else i for i in [opt_c_sending_time]][0]
        table[key][book_level - 1]['opt_c_sending_time'] = opt_c_sending_time
        fut_sending_time = table[key][book_level - 1]['fut_sending_time']
        fut_sending_time = [str(i) if str(i) != 'nan' else i for i in [fut_sending_time]][0]
        table[key][book_level - 1]['fut_sending_time'] = fut_sending_time
        table[key][book_level - 1]['timestamp'] = timestamp
        table[key][book_level - 1]['date'] = timestamp.date()
        table[key][book_level - 1]['year'] = timestamp.year
        table[key][book_level - 1]['month'] = timestamp.month_name()
        table[key][book_level - 1]['day'] = timestamp.day_name()
        table[key][book_level - 1]['hour'] = timestamp.hour
    return table


def rolling_liquidity( futures=None ,
                       options=None ,
                       times=None ,
                       rates=None ,
                       month_codes=None ,
                       book_level=1 ,
                       method=None ,
                       chunksize=25600 ):
    if times is None:
        fut_times = futures['sending_time']
        opt_times = options['sending_time']
        times = timetable(fut_times=fut_times , opt_times=opt_times , chunksize=chunksize)
    trade_days = __np__.unique(times['day'])
    if method == "hour":
        for day in trade_days:
            msg_day = times[__np__.where(times['day'] == day)]
            trade_hours = __np__.unique(msg_day['hours'])
            for hour in trade_hours:
                msg_hour = msg_day[__np__.where(msg_day['hours'] == hour)]
                if msg_hour.size != 0:
                    timestamp = max(msg_hour['timestamp'])
                    result = search_liquidity(futures=futures ,
                                              options=options ,
                                              month_codes=month_codes ,
                                              rates_table=rates ,
                                              timestamp=timestamp ,
                                              book_level=book_level)
                    yield result
    if method == "minute":
        for day in trade_days:
            msg_day = times[__np__.where(times['day'] == day)]
            trade_hours = __np__.unique(msg_day['hours'])
            for hour in trade_hours:
                msg_hour = msg_day[__np__.where(msg_day['hours'] == hour)]
                trade_minutes = __np__.unique(msg_day['minutes'])
                for minute in trade_minutes:
                    msg_minute = msg_day[__np__.where(msg_hour['minutes'] == minute)]
                    if msg_minute.size != 0:
                        timestamp = max(msg_minute['timestamp'])
                        result = search_liquidity(futures=futures ,
                                                  options=options ,
                                                  month_codes=month_codes ,
                                                  rates_table=rates ,
                                                  timestamp=timestamp ,
                                                  book_level=book_level)
                        yield result
    if method == "second":
        for day in trade_days:
            msg_day = times[__np__.where(times['day'] == day)]
            trade_hours = __np__.unique(msg_day['hours'])
            for hour in trade_hours:
                msg_hour = msg_day[__np__.where(msg_day['hours'] == hour)]
                if msg_hour.size != 0:
                    trade_minutes = __np__.unique(msg_day['minutes'])
                    for minute in trade_minutes:
                        msg_minute = msg_day[__np__.where(msg_hour['minutes'] == minute)]
                        if msg_minute.size != 0:
                            trade_sec = __np__.unique(msg_minute['seconds'])
                            for second in trade_sec:
                                msg_sec = msg_minute[__np__.where(msg_minute['seconds'] == second)]
                                if msg_sec.size != 0:
                                    timestamp = max(msg_sec['timestamp'])
                                    result = search_liquidity(futures=futures ,
                                                              options=options ,
                                                              month_codes=month_codes ,
                                                              rates_table=rates ,
                                                              timestamp=timestamp ,
                                                              book_level=book_level)
                                    yield result


def search_liquidity( futures=None ,
                      options=None ,
                      month_codes=None ,
                      rates_table=None ,
                      timestamp=None ,
                      book_level=None ):
    table = top_book(futures=futures , options=options , timestamp=timestamp , month_codes=month_codes)
    rate_dict = {}
    rates = rates_table.to_dict(orient='list')
    date = __datetime__.datetime(int(str(timestamp)[0:4]) , int(str(timestamp)[4:6]) , int(str(timestamp)[6:8]))
    for i, day in enumerate(rates[list(rates.keys())[0]]):
        day_time = __datetime__.datetime(int(day[0:4]) , int(day[5:7]) , int(day[8:10]))
        rate_dict[day_time] = rates[list(rates.keys())[1]][i]
    if date not in rate_dict.keys():
        date = date + __datetime__.timedelta(days=1)
    risk_rate = rate_dict[date]
    depth_total = 0
    spread_total = 0
    liquid_total = 0
    liquid_abs_total = 0
    liquid_spread_total = 0
    for k in table.keys():
        for i in range(book_level):
            exp_days = table[k][i]['exp_days']
            fut_bid = table[k][i]['fut_bid_price']
            fut_bid_size = table[k][i]['fut_bid_size']
            fut_bid_price = [__np__.NaN if __np__.isnan(p) else int(p) / 100 for p in [fut_bid]][0]
            fut_offer = table[k][i]['fut_offer_price']
            fut_offer_size = table[k][i]['fut_offer_size']
            fut_offer_price = [__np__.NaN if __np__.isnan(p) else int(p) / 100 for p in [fut_offer]][0]
            put_bid = table[k][i]['opt_p_bid_price']
            opt_p_bid_size = table[k][i]['opt_p_bid_size']
            put_bid_price = [__np__.NaN if __np__.isnan(p) else int(p) / 100 for p in [put_bid]][0]
            put_offer = table[k][i]['opt_p_offer_price']
            opt_p_offer_size = table[k][i]['opt_p_offer_size']
            put_offer_price = [__np__.NaN if __np__.isnan(p) else int(p) / 100 for p in [put_offer]][0]
            call_bid = table[k][i]['opt_c_bid_price']
            opt_c_bid_size = table[k][i]['opt_c_bid_size']
            call_bid_price = [__np__.NaN if __np__.isnan(p) else int(p) / 100 for p in [call_bid]][0]
            call_offer = table[k][i]['opt_c_offer_price']
            opt_c_offer_size = table[k][i]['opt_c_offer_size']
            call_offer_price = [__np__.NaN if __np__.isnan(p) else int(p) / 100 for p in [call_offer]][0]
            fut_price = (fut_bid_price + fut_offer_price) / 2
            put_price = (put_bid_price + put_offer_price) / 2
            call_price = (call_bid_price + call_offer_price) / 2
            share_strike = fut_price * __np__.exp(-risk_rate * (exp_days / 365))
            share_pv_strike = share_strike - (float(k) * __np__.exp(-risk_rate * (exp_days / 365)))
            share_strike = share_strike - float(k)
            put_call = call_price - put_price
            size_columns = [fut_bid_size , fut_offer_size ,
                            opt_p_bid_size , opt_c_bid_size ,
                            opt_p_offer_size , opt_c_offer_size]
            depth = min(size_columns)
            if put_call > share_pv_strike:
                pos_depth = [fut_bid_size , fut_offer_size , opt_p_offer_size , opt_c_bid_size]
                put_call_diff = put_call - share_pv_strike
                bid_offer_diff = ((call_bid - put_offer) / 100) - share_pv_strike
                spread_depth = min(pos_depth)
            elif put_call < share_strike:
                neg_depth = [fut_bid_size , fut_offer_size , opt_p_bid_size , opt_c_offer_size]
                put_call_diff = put_call - share_strike
                bid_offer_diff = share_strike - ((call_offer - put_bid) / 100)
                spread_depth = min(neg_depth)
            else:
                put_call_diff = 0
                bid_offer_diff = 0
                spread_depth = 0
            dd = {'fut_price_avg': (fut_offer_price + fut_bid_price) / 2 ,
                  'share_strike': share_strike ,
                  'share_pv_strike': share_pv_strike ,
                  'put_call': put_call ,
                  'put_call_diff': put_call_diff ,
                  'bid_offer_diff': bid_offer_diff ,
                  'spread_depth': spread_depth ,
                  'depth': depth}
            table[k][i].update(dd)
            depth_total += depth
            spread_total += spread_depth
    for k in table.keys():
        for i in range(book_level):
            bid_offer_diff = table[k][i]['bid_offer_diff']
            spread_depth = table[k][i]['spread_depth']
            put_call_diff = table[k][i]['put_call_diff']
            depth = table[k][i]['depth']
            if put_call_diff == 0:
                liquid = 0
                liquid_abs = 0
            else:
                liquid = (put_call_diff * depth) / depth_total
                liquid_abs = (abs(put_call_diff) * depth) / depth_total
            if bid_offer_diff == 0 or spread_depth == 0:
                liquid_diff = 0
            else:
                liquid_diff = (bid_offer_diff * spread_depth) / spread_total
            liquid_diff = [liquid_diff if liquid_diff > 0 else 0.0][0]
            dd = {'liquid': liquid ,
                  'liquid_abs': liquid_abs ,
                  'liquid_spread': liquid_diff ,
                  'depth_total': depth_total ,
                  'spread_total': spread_total}
            table[k][i].update(dd)
            liquid_total += liquid
            liquid_abs_total += liquid_abs
            liquid_spread_total += liquid_diff
    for k in table.keys():
        for i in range(book_level):
            dd = {'liquid_total': liquid_total ,
                  'liquid_abs_total': liquid_abs_total ,
                  'liquid_spread_total': liquid_spread_total}
            table[k][i].update(dd)
    return table
