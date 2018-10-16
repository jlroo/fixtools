# -*- coding: utf-8 -*-

import os as __os__
import datetime as __datetime__
import pandas as __pd__
import numpy as __np__
import multiprocessing as __mp__
from fixtools.util.util import files_tree , contract_code
from fixtools.util.parity import rolling_liquidity , search_liquidity


def __time__( timestamp ):
    date = __datetime__.datetime.strptime(str(timestamp) , "%Y%m%d%H%M%S%f")
    dd = (date.year , date.month , date.day , date.hour , date.minute ,
          date.second , date.microsecond * 0.001 , date.microsecond , int(timestamp))
    return dd


def timetable( fut_times=None , opt_times=None , chunksize=25600 ):
    sending_time = __np__.concatenate([fut_times , opt_times])
    sending_time = __np__.unique(sending_time)
    with __mp__.Pool() as pool:
        timemap = pool.map(__time__ , sending_time , chunksize=chunksize)
    times = __np__.array(timemap , dtype=[('year' , '>i2') , ('month' , '>i2') , ('day' , '>i2') ,
                                          ('hours' , '>i2') , ('minutes' , '>i2') , ('seconds' , '>i2') ,
                                          ('milliseconds' , '>i4') , ('microsecond' , '>i4') , ('timestamp' , '>i8')])
    return times


def books_locator( exchange=None ,
                   year=None ,
                   asset=None ,
                   product=None ,
                   month=None ,
                   instrument=None ,
                   local_path=None ,
                   top_books=True ):
    """
    locates where the order books are in the local or cloud database
    :param exchange:
    :param year:
    :param asset:
    :param product:
    :param month:
    :param instrument:
    :param local_path:
    :param top_books:
    :return:
    """
    files = {}
    year = str(year)
    exchange = exchange.upper()
    asset = asset.upper()
    product = product.upper()
    instrument = instrument.upper()
    if type(month) == str:
        months = {'january': 1 , 'february': 2 , 'march': 3 , 'april': 4 , 'may': 5 , 'june': 6 ,
                  'july': 7 , 'august': 8 , 'september': 9 , 'october': 10 , 'november': 11 , 'december': 12}
        month = {'n': months[k] for k in months.keys() if month.lower() in k}
        if month == {}:
            raise ValueError("Enter a correct month")
    elif type(month) == int:
        if month not in range(1 , 13):
            raise ValueError("Month should be an integer between 1 to 12")
        month = {'n': month}
    exchange = "exchange_" + exchange
    year = "/year_" + year
    asset = "/asset_" + asset
    product = "/product_" + product
    instrument = "/instrument_" + instrument
    month = "/month_" + contract_code(month=month['n'] , path_code=True)[0]
    if top_books:
        md_path = "/md_BOOKS_TOP" + month
    else:
        md_path = "/md_BOOKS" + month
    if local_path:
        local_path = str([item + "/" if item[-1] != "/" else item for item in [local_path]][0])
        path = local_path + exchange + year + asset + product + md_path + instrument + "/"
        src_files = list(__os__.walk(path))[0][2]
        for file in src_files:
            key = int(file.split("-")[0])
            if key not in files.keys():
                files[key] = []
                files[key].append(path + file)
            else:
                files[key].append(path + file)
    else:
        # TODO: Implement cloud-DB version to pull data from sbecipher could
        print("need cloud implementation")
    return files


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
    for key in fixfiles.keys():
        opt_file = fixfiles[key]['options'][0]
        options = __np__.load(file=path_month + opt_file)
        fut_file = fixfiles[key]['futures'][0]
        futures = __np__.load(file=path_month + fut_file)
        time_file = timefiles[key]['futures'][0]
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
