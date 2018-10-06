# -*- coding: utf-8 -*-

import datetime as __datetime__
import pandas as __pd__
import numpy as __np__
import multiprocessing as __mp__
import pickle
from os.path import getsize
from collections import defaultdict
from fixtools.util.util import expiration_date , open_fix
from fixtools.io.fixfast import FixDict , files_tree


# TODO: look into FixDict class on fixfast.py make it more robust


def book_table( path=None ,
                path_out=None ,
                file_name=None ,
                product=None ,
                num_orders=1 ,
                chunksize=25600 ,
                read_ram=True ):
    """
    Function to convert fix books to pandas dataframe
    :param path: Location of FIX order books
    :param path_out: Location for the pandas order books
    :param file_name: File or files names to be process
    :param product: Type of product to be process futures/options
    :param num_orders: Order depth, number of book orders
    :param chunksize: Number of lines to process
    :param read_ram: If true will read all the data to ram
    :return: Pandas dataframe FIX Book order
    """
    if product is None:
        raise ValueError("Product cant be None. Types: futures or options")
    elif product not in "opt|options" and product not in "fut|futures":
        raise ValueError("Product Types: futures or options")
    path = str([item + "/" if item[-1] != "/" else item for item in [path]][0])
    dfs = []
    for item in iter(file_name):
        file_path = path + item
        if getsize(file_path) == 0:
            dfs.append(__pd__.DataFrame())
        else:
            # Fixdict class from FIX to dictionary
            fix_dict = FixDict(num_orders=num_orders)
            fixdata = open_fix(file_path , compression=False)
            if read_ram:
                data = fixdata.data.readlines()
            else:
                data = fixdata.data
            with __mp__.Pool() as pool:
                df = pool.map(fix_dict.to_dict , data , chunksize=chunksize)
                dfs.append(__pd__.DataFrame.from_dict(df))
            try:
                data.close()
            except AttributeError:
                pass
    try:
        contract_book = __pd__.concat(dfs)
        contract_book = contract_book.replace('NA' , __np__.nan)
    except ValueError:
        contract_book = __pd__.DataFrame()
    if path_out:
        path_out = str([item + "/" if item[-1] != "/" else item for item in [path_out]][0])
        if product in "opt|options":
            file_name = path_out + file_name[0][:-5] + "OPTIONS.csv"
        elif product in "fut|futures":
            file_name = path_out + file_name[0] + ".csv"
        contract_book.to_csv(file_name , index=False)
    return contract_book


def __timemap__( timestamp ):
    date = __datetime__.datetime.strptime(str(timestamp) , "%Y%m%d%H%M%S%f")
    dd = {"date": int(str(date)[0:10].replace("-" , "")) ,
          "hour": date.hour ,
          "timestamp": timestamp}
    return dd


def __depth__( depth_func="min" , size=None ):
    if depth_func == "max":
        return max(size)
    elif depth_func == "min":
        return min(size)


def __time__( timestamp ):
    date = __datetime__.datetime.strptime(str(timestamp) , "%Y%m%d%H%M%S%f")
    dd = {"ymd": int(date.date().strftime("%Y%m%d")) ,
          "hr": date.hour ,
          "mnt": date.minute ,
          "sec": date.second ,
          "timestamp": timestamp}
    return dd


def timetable( fut_timestamp , opt_timestamp , chunksize=25600 ):
    with __mp__.Pool() as pool:
        fut_times = pool.map(__time__ , fut_timestamp , chunksize=chunksize)
        opt_times = pool.map(__time__ , opt_timestamp , chunksize=chunksize)
        dd = {}
    for h in range(0 , 24):
        dd[h] = {}
        for m in range(0 , 60):
            dd[h][m] = {}
            for s in range(0 , 60):
                dd[h][m][s] = {}
    times = {"futures": {} , "options": {}}
    for item in fut_times:
        ymd = item["ymd"]
        hr = item["hr"]
        mnt = item["mnt"]
        sec = item["sec"]
        if ymd not in times["futures"].keys():
            times["futures"][ymd] = dd
            times["futures"][ymd][hr][mnt] = defaultdict(list)
            times["futures"][ymd][hr][mnt][sec].append(item["timestamp"])
        else:
            times["futures"][ymd][hr][mnt][sec].append(item["timestamp"])
    for item in opt_times:
        ymd = item["ymd"]
        hr = item["hr"]
        mnt = item["mnt"]
        sec = item["sec"]
        if ymd not in times["options"].keys():
            times["options"][ymd] = dd
            times["options"][ymd][hr][mnt] = defaultdict(list)
            times["options"][ymd][hr][mnt][sec].append(item["timestamp"])
        else:
            times["options"][ymd][hr][mnt][sec].append(item["timestamp"])
    return times


def time_table( fut_timestamp , opt_timestamp , chunksize=25600 ):
    with __mp__.Pool() as pool:
        fut_times = pool.map(__timemap__ , fut_timestamp , chunksize=chunksize)
        opt_times = pool.map(__timemap__ , opt_timestamp , chunksize=chunksize)
    grouped = {"futures": {}, "options": {}}
    for item in fut_times:
        ymd = item["date"]
        if ymd not in grouped["futures"].keys():
            grouped["futures"][ymd] = defaultdict(list)
            grouped["futures"][ymd][item["hour"]].append(item["timestamp"])
        else:
            grouped["futures"][ymd][item["hour"]].append(item["timestamp"])
    for item in opt_times:
        ymd = item["date"]
        if ymd not in grouped["options"].keys():
            grouped["options"][ymd] = defaultdict(list)
            grouped["options"][ymd][item["hour"]].append(item["timestamp"])
        else:
            grouped["options"][ymd][item["hour"]].append(item["timestamp"])
    return grouped


def search_csv( path=None ,
                path_out=None ,
                path_times=None ,
                df_rates=None ,
                df_futures=None ,
                df_options=None ,
                times_dict=None ,
                columns=None ,
                chunksize=25600 ):
    """
    Search_csv function to search the order book and calculate liquidity
    :param path:
    :param path_out:
    :param path_times:
    :param df_rates:
    :param df_futures:
    :param df_options:
    :param times_dict:
    :param columns:
    :param chunksize:
    :return:
    """
    fixfiles = files_tree(path)
    timefiles = files_tree(path_times)
    for key in fixfiles.keys():
        opt_file = fixfiles[key]['options'][0]
        options = __pd__.read_csv(path + opt_file)
        fut_file = fixfiles[key]['futures'][0]
        futures = __pd__.read_csv(path + fut_file)
        if times_dict is None and path_times is None:
            if float(__pd__.__version__[2:]) >= 23.0:
                fut_timestamp = futures.sending_time.values
                opt_timestamp = options.sending_time.values
            else:
                fut_timestamp = futures.sending_time.as_matrix()
                opt_timestamp = options.sending_time.as_matrix()
            times_dict = time_table(fut_timestamp , opt_timestamp , chunksize=chunksize)
        if path_times:
            path_times = str([item + "/" if item[-1] != "/" else item for item in [path_times]][0])
            time_file = timefiles[key]['futures'][0]
            with open(path_times + time_file , 'rb') as handle:
                times_dict = pickle.load(handle)
        for date in times_dict['futures'].keys():
            for hour in times_dict['futures'][date].keys():
                opt_time = times_dict['options'][date][hour][-1]
                fut_time = times_dict['futures'][date][hour][-1]
                timestamp = [fut_time if fut_time > opt_time else opt_time][0]
                result = liquidity(futures , options , df_rates , timestamp)
                if not result == {}:
                    search_out(result , timestamp , path_out , col_order=columns)
                    print("[DONE] -- FUT -- " + fut_file + " -- " + timestamp)
    if df_futures and df_options:
        if times_dict is None:
            if float(__pd__.__version__[2:]) >= 23.0:
                fut_timestamp = df_futures.sending_time.values
                opt_timestamp = df_options.sending_time.values
            else:
                fut_timestamp = df_futures.sending_time.as_matrix()
                opt_timestamp = df_options.sending_time.as_matrix()
            times_dict = time_table(fut_timestamp , opt_timestamp , chunksize=chunksize)
        for date in times_dict['futures'].keys():
            for hour in times_dict['futures'][date].keys():
                opt_time = times_dict['options'][date][hour][-1]
                fut_time = times_dict['futures'][date][hour][-1]
                timestamp = [fut_time if fut_time > opt_time else opt_time][0]
                parity_result = liquidity(df_futures , df_options , df_rates , timestamp)
                if not parity_result == {}:
                    search_out(parity_result , timestamp , path_out , col_order=columns)


def search_fix( path=None , path_out=None , path_parity=None , path_times=None , df_rates=None ,
                columns=None , num_orders=1 , chunksize=25600 , read_ram=True , parity_check=False ):
    """
    Create csv files from FIX Order Books
    :param path:
    :param path_out:
    :param path_parity:
    :param path_times:
    :param df_rates:
    :param columns:
    :param num_orders:
    :param chunksize:
    :param read_ram:
    :param parity_check:
    :return:
    """
    fixfiles = files_tree(path)
    if path_times is None:
        path_times = ""
    else:
        path_times = str([item + "/" if item[-1] != "/" else item for item in [path_times]][0])
    for key in fixfiles.keys():
        opt_files = fixfiles[key]['options']
        # fix options books to pandas dataframe
        options = book_table(path=path , path_out=path_out , file_name=opt_files , product="options" ,
                             num_orders=num_orders, chunksize=chunksize, read_ram=read_ram)
        print("[DONE] -- " + str(key).zfill(3) + " -- " + opt_files[0][:-5] + "OPTIONS")
        fut_file = fixfiles[key]['futures']
        # fix futures book to pandas dataframe
        futures = book_table(path=path, path_out=path_out, file_name=fut_file, product="futures",
                             num_orders=num_orders, chunksize=chunksize, read_ram=read_ram)
        print("[DONE] -- " + str(key).zfill(3) + " -- " + fut_file[0] + "-FUTURES")
        if float(__pd__.__version__[2:]) >= 23.0:
            fut_timestamp = futures.sending_time.values
            opt_timestamp = options.sending_time.values
        else:
            fut_timestamp = futures.sending_time.as_matrix()
            opt_timestamp = options.sending_time.as_matrix()
        times = time_table(fut_timestamp , opt_timestamp , chunksize=chunksize)
        filename = path_times + fut_file[0] + '.pickle'
        with open(filename , 'wb') as handle:
            pickle.dump(times, handle, protocol=pickle.HIGHEST_PROTOCOL)
        if parity_check:
            if not futures.empty and not options.empty:
                search_csv(path_out=path_parity, df_rates=df_rates, df_futures=futures, df_options=options,
                           times_dict=times, columns=columns, chunksize=chunksize)
                print("[DONE] -- " + str(key).zfill(3) + " -- " + fut_file[0] + " -- PARITY CHECK")


def search_out( result=None , timestamp=None , path_out=None , col_order=None , df_return=False ):
    """
    Write book order to csv file
    :param result:
    :param timestamp:
    :param path_out:
    :param col_order:
    :param df_return:
    :return: Save file as csv
    """
    col_default = ['share_strike' , 'put_call' , 'share_pv_strike' , 'put_call_diff' , 'strike_price' , 'trade_date' ,
                   'exp_date' , 'exp_days' , 'fut_bid_price' , 'opt_p_bid_price' , 'opt_c_bid_price' ,
                   'fut_offer_price' ,
                   'opt_p_offer_price' , 'opt_c_offer_price' , 'fut_msg_seq_num' , 'opt_p_msg_seq_num' ,
                   'opt_c_msg_seq_num' , 'fut_sending_time' , 'opt_p_sending_time' , 'opt_c_sending_time' ,
                   'fut_bid_size' ,
                   'opt_p_bid_size' , 'opt_c_bid_size' , 'fut_offer_size' , 'opt_p_offer_size' , 'opt_c_offer_size' ,
                   'fut_bid_level' , 'opt_p_bid_level' , 'opt_p_offer_level' , 'fut_offer_level' , 'opt_c_bid_level' ,
                   'opt_c_offer_level' , 'fut_sec_id' , 'opt_p_sec_id' , 'opt_c_sec_id' , 'fut_sec_desc' ,
                   'opt_p_desc' ,
                   'opt_c_desc']
    path_out = str([item + "/" if item[-1] != "/" else item for item in [path_out]][0])
    file_name = path_out + str(timestamp) + ".csv"
    timestamp = __pd__.Timestamp(year=int(timestamp[0:4]) , month=int(timestamp[4:6]) ,
                                 day=int(timestamp[6:8]) , hour=int(timestamp[8:10]) ,
                                 minute=int(timestamp[10:12]) , second=int(timestamp[12:14]) ,
                                 microsecond=int(timestamp[14:]) * 1000 , unit="ms").ceil("H")
    df = []
    for k in result.keys():
        df.append(__pd__.DataFrame.from_dict(result[k] , orient='index'))
    df = __pd__.concat(df)
    df.reset_index()
    df['opt_p_sending_time'] = [str(i) if str(i) != 'nan' else i for i in df['opt_p_sending_time']]
    df['opt_c_sending_time'] = [str(i) if str(i) != 'nan' else i for i in df['opt_c_sending_time']]
    df['fut_sending_time'] = [str(i) if str(i) != 'nan' else i for i in df['fut_sending_time']]
    df['timestamp'] = timestamp
    df['date'] = timestamp.date()
    df['year'] = timestamp.year
    df['month'] = timestamp.month_name()
    df['day'] = timestamp.day_name()
    df['hour'] = timestamp.hour
    if col_order:
        df = df[col_order]
    else:
        df = df[col_default]
    df.to_csv(file_name , index=False , quotechar='"')
    if df_return:
        return df


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
    if order_type == "C":
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
    dict_table = {"fut": []}
    codes = {k[1]: k[0] for k in enumerate(month_codes.rsplit(",") , 1)}
    query = futures[(futures['bid_level'] <= top_book)]
    query = query[__pd__.notnull(query['security_desc'])]
    query = query[query['sending_time'] <= int(timestamp)]
    query = query.sort_values('msg_seq_num')
    query = query.reset_index()
    del query['index']
    fut_dict = query.tail(top_book).to_dict(orient="records")
    for item in fut_dict:
        dd = __bookdict__(item , codes)
        dict_table["fut"].append(dd.copy())
    query = options[(options['bid_level'] <= top_book)]
    query = query[__pd__.notnull(query['security_desc'])]
    query = query[query['sending_time'] <= int(timestamp)]
    query = query.sort_values('msg_seq_num')
    query = query.reset_index()
    del query['index']
    opts = list(query['security_id'].unique())
    for sec in opts:
        sec_query = query[query['security_id'] == sec]
        item = sec_query.tail(top_book).to_dict(orient="records")[-1]
        sec_desc = str(item['security_desc'])
        price = int(sec_desc.split(" ")[1][1:])
        if price not in dict_table.keys():
            dict_table[price] = {i: {} for i in range(book_level)}
            dd = __orderdict__(item , codes)
            table_dd = dict_table["fut"][book_level - 1].copy()
            table_dd.update(dd)
            dict_table[price][book_level - 1] = table_dd.copy()
        else:
            dd = __orderdict__(item , codes)
            dict_table[price][book_level - 1].update(dd)
    del dict_table["fut"]
    return dict_table


def liquidity( futures=None ,
               options=None ,
               rates_table=None ,
               timestamp=None ,
               month_codes=None ,
               level_limit=1 ,
               depth_func="min" ):
    """
    Calculate liquidity
    :param futures:
    :param options:
    :param rates_table:
    :param timestamp:
    :param month_codes:
    :param level_limit:
    :param depth_func:
    :return:
    """
    table = top_book(futures=futures , options=options , timestamp=timestamp , month_codes=month_codes)
    rate_dict = {}
    rates = rates_table.to_dict(orient='list')
    date = __datetime__.datetime(int(timestamp[0:4]) , int(timestamp[4:6]) , int(timestamp[6:8]))
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
        for i in range(level_limit):
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
            depth = __depth__(depth_func=depth_func , size=size_columns)
            if put_call > share_pv_strike:
                pos_depth = [fut_bid_size , fut_offer_size , opt_p_offer_size , opt_c_bid_size]
                put_call_diff = put_call - share_pv_strike
                bid_offer_diff = ((call_bid - put_offer) / 100) - share_pv_strike
                spread_depth = __depth__(depth_func=depth_func , size=pos_depth)
            elif put_call < share_strike:
                neg_depth = [fut_bid_size , fut_offer_size , opt_p_bid_size , opt_c_offer_size]
                put_call_diff = put_call - share_strike
                bid_offer_diff = share_strike - ((call_offer - put_bid) / 100)
                spread_depth = __depth__(depth_func=depth_func , size=neg_depth)
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
        for i in range(level_limit):
            bid_offer_diff = table[k][i]['bid_offer_diff']
            spread_depth = table[k][i]['spread_depth']
            put_call_diff = table[k][i]['put_call_diff']
            depth = table[k][i]['depth']
            liquid = (put_call_diff * depth) / depth_total
            liquid_abs = (abs(put_call_diff) * depth) / depth_total
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
        for i in range(level_limit):
            dd = {'liquid_total': liquid_total ,
                  'liquid_abs_total': liquid_abs_total ,
                  'liquid_spread_total': liquid_spread_total}
            table[k][i].update(dd)
    return table
