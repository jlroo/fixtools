# -*- coding: utf-8 -*-

import pandas as __pd__
import numpy as __np__
import datetime as __datetime__

from fixtools.core.book import top_book
from fixtools.util.util import files_tree , timetable


def weekly_liquidity( path_month=None ,
                      path_out=None ,
                      path_times=None ,
                      df_rates=None ,
                      frequency='hour' ,
                      chunksize=25600 ):
    path_month = str([item + "/" if item[-1] != "/" else item for item in [path_month]][0])
    fixfiles = files_tree(path_month)
    path_times = str([item + "/" if item[-1] != "/" else item for item in [path_times]][0])
    timefiles = files_tree(path_times)
    if len(fixfiles['futures'].keys()) != len(fixfiles['options'].keys()):
        raise ValueError("Number of files per week is different between futures and options")
    else:
        num_weeks = len(fixfiles['futures'].keys())
    for key in range(num_weeks):
        opt_file = fixfiles['options'][key][0]
        options = __np__.load(file=path_month + opt_file)
        fut_file = fixfiles['futures'][key][0]
        futures = __np__.load(file=path_month + fut_file)
        time_file = timefiles['futures'][key][0]
        times = __np__.load(file=time_file)
        results = rolling_liquidity(futures=futures ,
                                    options=options ,
                                    times=times ,
                                    rates=df_rates ,
                                    month_codes=None ,
                                    book_level=1 ,
                                    method=frequency ,
                                    chunksize=chunksize)
        dict_list = []
        for result in results:
            if not result == {}:
                for k in result.keys():
                    df = __pd__.DataFrame.from_dict(result[k] , orient='index')
                    dict_list.append(df)
        data = __pd__.concat(dict_list)
        data.reset_index()
        path_out = str([item + "/" if item[-1] != "/" else item for item in [path_out]][0])
        file_name = path_out + fut_file + "-liquidity" + ".csv"
        data.to_csv(file_name , index=False , quotechar='"')
        print("[DONE] -- LIQUIDITY -- " + fut_file)
        return data


def timestamp_liquidity( futures=None ,
                         options=None ,
                         timestamp=None ,
                         df_rates=None ,
                         path_out=None ):
    query = search_liquidity(futures=futures , options=options , rates_table=df_rates , timestamp=timestamp ,
                             book_level=1)
    dict_list = []
    for item in query:
        if not item == {}:
            for k in item.keys():
                df = __pd__.DataFrame.from_dict(item[k] , orient='index')
                dict_list.append(df)
    data = __pd__.concat(dict_list)
    data.reset_index()
    path_out = str([item + "/" if item[-1] != "/" else item for item in [path_out]][0])
    file_name = path_out + str(timestamp) + "-liquidity" + ".csv"
    data.to_csv(file_name , index=False , quotechar='"')
    print("[DONE] -- LIQUIDITY -- " + str(timestamp))
    return data


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
    for i , day in enumerate(rates[list(rates.keys())[0]]):
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