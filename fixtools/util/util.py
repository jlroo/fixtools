"""
Created on Fri Jul 22 17:33:13 2016
@author: jlroo
"""

import os as __os__
import bz2 as __bz2__
import calendar as __calendar__
import datetime as __datetime__
import multiprocessing as __mp__
import gzip as __gzip__
import numpy as __np__
from collections import defaultdict
from fixtools.io.fixfast import FixData , FixStruct
from os.path import getsize


def open_fix( path , period="weekly" , compression=True ):
    """
    Def FixData This class returns that number a report of the fix data
    contracts and volume.
    :param path:
    :param period:
    :param compression:
    :return:
    """
    if period.lower() not in ("weekly" , "daily" , "monthly"):
        raise ValueError("Supported time period: weekly or daily")
    src = {"path": path , "period": period.lower()}
    if compression is False:
        if path[-4:].lower in (".zip" , ".tar"):
            raise ValueError("Supported compressions gzip, bz2 or bytes data")
        else:
            fixfile = open(path , 'rb')
    else:
        if path[-3:] == ".gz":
            fixfile = __gzip__.open(path , 'rb')
        elif path[-4:] == ".bz2":
            fixfile = __bz2__.BZ2File(path , 'rb')
        else:
            raise ValueError("Supported files gzip,bz2, uncompress bytes file. \
            For uncompressed files change compression flag to False.")
    return FixData(fixfile , src)


def files_tree( path ):
    files_list = list(__os__.walk(path))[0][2]
    files = {'futures': defaultdict(list) , 'options': defaultdict(list)}
    for file in files_list:
        key = int(file.split("-")[0])
        if "c" in file.lower() or "p" in file.lower():
            files["options"][key].append(file)
        else:
            files["futures"][key].append(file)
    return files


def data_dates( data_line , period="weekly" ):
    peek = data_line.split(b"\n")[0]
    day0 = peek[peek.find(b'\x0152=') + 4:peek.find(b'\x0152=') + 12]
    start = __datetime__.datetime(year=int(day0[:4]) , month=int(day0[4:6]) , day=int(day0[6:8]))
    if period == "weekly":
        dates = [start + __datetime__.timedelta(days=i) for i in range(6)]
        return dates


def settlement_day( date , week_number , day_of_week ):
    weekday = {'monday': 0 , 'tuesday': 1 , 'wednesday': 2 , 'thursday': 3 , 'friday': 4 , 'saturday': 5 , 'sunday': 6}
    date = __datetime__.datetime(date.year , date.month , date.day)
    if date.weekday() == weekday[day_of_week.lower()]:
        if date.day // 7 == (week_number - 1):
            return True
    return False


def expiration_date( year=None , month=None , week=None , day=None ):
    if not day:
        day = "friday"
    weekday = {'monday': 0 , 'tuesday': 1 , 'wednesday': 2 ,
               'thursday': 3 , 'friday': 4 , 'saturday': 5 , 'sunday': 6}
    weeks = __calendar__.monthcalendar(year , month)
    exp_day = week - 1
    if weeks[0][-1] == 1:
        exp_week = week
    else:
        exp_week = week - 1
    for dd in weeks[exp_week]:
        date = __datetime__.datetime(year , month , dd)
        if date.weekday() == weekday[day.lower()]:
            if date.day // 7 == exp_day:
                return date


def market_calendar( year=None , firstweekday=6 ):
    """
    creates a calendar for the market year
    :param year:
    :param firstweekday:
    :return:
    """
    mrkt_calendar = __calendar__.Calendar(firstweekday=firstweekday).yeardatescalendar(year=year)
    mrktyear = {'metadata': {} , 'calendar': {}}
    mnt = 1
    for quarter , months in enumerate(mrkt_calendar , 1):
        quarter_name = 'Q' + str(quarter)
        qrt = quarter * 3
        q = contract_code(month=qrt , path_code=True)[0]
        if quarter == 1:
            quarter_start = expiration_date(year=year - 1 , month=12 , week=3)
        else:
            quarter_start = mrktyear['metadata']['Q' + str(quarter - 1)]['end_month']
        quarter_end = expiration_date(year=year , month=qrt , week=3)
        mrktyear['calendar'][quarter] = {}
        mrktyear['metadata'][quarter_name] = {'month_code': q ,
                                              'start_month': quarter_start ,
                                              'end_month': quarter_end}
        for month in months:
            m = contract_code(month=mnt , path_code=True)[1]
            if mnt == 1:
                mnt_start = expiration_date(year=year - 1 , month=12 , week=3)
            else:
                mnt_start = expiration_date(year=year , month=mnt - 1 , week=3)
            mnt_end = expiration_date(year=year , month=mnt , week=3)
            mrktyear['metadata'][quarter_name][mnt] = {'month_code': m ,
                                                       'start_month': mnt_start ,
                                                       'end_month': mnt_end ,
                                                       'num_weeks': len(month)}
            mrktyear['calendar'][quarter][mnt] = {}
            for n , wk in enumerate(month , 1):
                mrktyear['calendar'][quarter][mnt][n] = wk
            mnt += 1
    return mrktyear


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


def contract_code( month=None , codes=None , path_code=False , return_table=False ):
    """
    Contact_code returns the cme code for the give month
    :param month:
    :param codes:
    :param path_code:
    :param return_table:
    :return:
    """
    if not codes:
        codes = "F,G,H,J,K,M,N,Q,U,V,X,Z,F,G,H,J,K,M,N,Q,U,V,X,Z"
    mnt_codes = {k[0]: k[1] for k in enumerate(codes.rsplit(",") , 1)}
    mnt_table = {}
    for key in mnt_codes:
        if key % 3 == 0:
            idx = mnt_codes[key]
            if path_code:
                mnt_table[key] = (idx + idx , {key - 2: idx + mnt_codes[key - 2] , key - 1: idx + mnt_codes[key - 1]})
            else:
                mnt_table[key] = (idx , {key - 2: mnt_codes[key - 2] , key - 1: mnt_codes[key - 1]})
    if month % 3 == 0:
        return mnt_table[month][0]
    if month % 3 == 1:
        return mnt_table[month + 2][1][month]
    if month % 3 == 2:
        return mnt_table[month + 1][1][month]
    if return_table:
        return mnt_table


def most_liquid( ymd=None ,
                 data_line=None ,
                 product=None ,
                 instrument=None ,
                 code_year=None ,
                 cme_codes=None ,
                 other_codes=None ):
    """
    most_liquid to determine the most liquid contract on a fixfile or date (ymd)
    :param ymd:
    :param data_line:
    :param product:
    :param instrument:
    :param code_year:
    :param cme_codes:
    :param other_codes:
    :return:
    """
    if data_line:
        day0 = data_line[data_line.find(b'\x0152=') + 4:data_line.find(b'\x0152=') + 12]
    else:
        day0 = ymd
    start = __datetime__.datetime(year=int(day0[:4]) , month=int(day0[4:6]) , day=int(day0[6:8]))
    dates = [start + __datetime__.timedelta(days=i) for i in range(6)]
    if cme_codes:
        codes = "F,G,H,J,K,M,N,Q,U,V,X,Z,F,G,H,J,K,M,N,Q,U,V,X,Z"
    else:
        codes = other_codes
    sec_code = ""
    date = __datetime__.datetime(year=dates[0].year , month=dates[0].month , day=dates[0].day)
    exp_week = filter(lambda day: settlement_day(day , 3 , 'friday') , dates)
    expired = True if date.day > 16 else False
    if exp_week is not None or expired:
        if instrument.lower() in ("fut" , "futures"):
            if date.month % 3 == 0:
                sec_code = contract_code(date.month + 3 , codes)
            if date.month % 3 == 1:
                sec_code = contract_code(date.month + 2 , codes)
            if date.month % 3 == 2:
                sec_code = contract_code(date.month + 1 , codes)
        if instrument.lower() in ("opt" , "options"):
            sec_code = contract_code(date.month + 1 , codes)
    else:
        if instrument.lower() in ("fut" , "futures"):
            if date.month % 3 == 0:
                sec_code = contract_code(date.month , codes)
            if date.month % 3 == 1:
                sec_code = contract_code(date.month + 2 , codes)
            if date.month % 3 == 2:
                sec_code = contract_code(date.month + 1 , codes)
        if instrument.lower() in ("opt" , "options"):
            sec_code = contract_code(date.month , codes)
    sec_desc = product + sec_code + code_year
    return sec_desc


def security_description( data , group="ES" , group_code="EZ" , max_lines=10000 ):
    description = []
    code = (group.encode() , group_code.encode())
    for cnt , line in enumerate(data):
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
            description.append({sec_id: sec_desc})
    try:
        data.close()
    except AttributeError:
        pass
    return description


def contracts( description ):
    securities = {}
    months = set("F,G,H,J,K,M,N,Q,U,V,X,Z".split(","))
    filtered = {list(item.keys())[0]: list(item.values())[0] for item in description}
    for sec_id in filtered.keys():
        sec_desc = filtered[sec_id]
        if len(sec_desc) < 12:
            sec_key = sec_desc[0:4]
            if sec_key not in securities.keys():
                securities[sec_key] = {"FUT": {} , "OPT": {} , "PAIRS": {} , "SPREAD": {}}
            for month in months:
                if month in sec_desc:
                    if len(sec_desc) < 7:
                        securities[sec_key]['FUT'][sec_id] = sec_desc
                    if 'P' in sec_desc or 'C' in sec_desc:
                        securities[sec_key]['OPT'][sec_id] = sec_desc
                        if 'C' in sec_desc:
                            call_price = int(sec_desc.split(" C")[-1])
                            if call_price not in securities[sec_key]['PAIRS'].keys():
                                securities[sec_key]['PAIRS'][call_price] = {}
                                securities[sec_key]['PAIRS'][call_price][sec_id] = sec_desc
                            else:
                                securities[sec_key]['PAIRS'][call_price][sec_id] = sec_desc
                        if "P" in sec_desc:
                            put_price = int(sec_desc.split(" P")[-1])
                            if put_price not in securities[sec_key]['PAIRS'].keys():
                                securities[sec_key]['PAIRS'][put_price] = {}
                                securities[sec_key]['PAIRS'][put_price][sec_id] = sec_desc
                            else:
                                securities[sec_key]['PAIRS'][put_price][sec_id] = sec_desc
                    if '-' in sec_desc:
                        securities[sec_key]['SPREAD'][sec_id] = sec_desc
    for sec_key in securities.keys():
        pairs = securities[sec_key]['PAIRS'].copy()
        for price in pairs.keys():
            if len(pairs[price]) != 2:
                del securities[sec_key]['PAIRS'][price]
    return securities


def liquid_securities( fixdata=None ,
                       instrument=None ,
                       group_code=None ,
                       code_year=None ,
                       products=None ,
                       cme_codes=True ,
                       max_lines=50000 ):
    if products is None:
        products = ["FUT" , "OPT"]
    if instrument is None:
        instrument = "ES"
    if group_code is None:
        group_code = "EZ"
    description = security_description(fixdata , instrument , group_code , max_lines)
    securities = contracts(description)
    liquid = {}
    fix_line = fixdata[0].split(b"\n")[0]
    fut = most_liquid(fix_line , instrument , products[0] , code_year , cme_codes)
    opt = most_liquid(fix_line , instrument , products[1] , code_year , cme_codes)
    liquid.update(securities[fut][products[0]])
    for price in securities[opt]["PAIRS"].keys():
        liquid.update(securities[opt]['PAIRS'][price])
    return liquid


def _topbook( item=None ):
    security_id = item.split(b"\x0148=")[1].split(b"\x01")[0].decode()
    trade_date = item.split(b"\x0175=")[1].split(b"\x01")[0].decode()
    security_desc = item.split(b"\x01107=")[1].split(b"\x01")[0].decode()
    msg_seq_num = int(item.split(b"\x0134=")[1].split(b"\x01")[0].decode())
    sending_time = int(item.split(b"\x0152=")[1].split(b"\x01")[0].decode())
    body = item.split(b'\x0110=')[0].split(b'\x01279')[1:]
    bids = body[:len(body) // 2]
    offers = body[len(body) // 2:]
    bid_price = bids[0].split(b'\x01270=')[1].split(b'\x01')[0].decode()
    bid_size = bids[0].split(b'\x01271=')[1].split(b'\x01')[0].decode()
    offer_price = offers[0].split(b'\x01270=')[1].split(b'\x01')[0].decode()
    offer_size = offers[0].split(b'\x01271=')[1].split(b'\x01')[0].decode()
    top = (msg_seq_num , security_id , sending_time , trade_date , bid_price , bid_size , offer_price , offer_size)
    return security_desc , top


def weekly_orderbooks( path_files=None ,
                       path_out=None ,
                       path_times=None ,
                       num_orders=1 ,
                       chunksize=25600 ,
                       read_ram=True ):
    path_files = str([item + "/" if item[-1] != "/" else item for item in [path_files]][0])
    path_times = str([item + "/" if item[-1] != "/" else item for item in [path_times]][0])
    path_out = str([item + "/" if item[-1] != "/" else item for item in [path_out]][0])
    fixfiles = files_tree(path_files)
    if len(fixfiles['futures'].keys()) != len(fixfiles['options'].keys()):
        raise ValueError("Number of files per week is different between futures and options")
    else:
        num_weeks = len(fixfiles['futures'].keys())
    for key in range(num_weeks):
        opt_files = fixfiles['options'][key]
        options = booktable(path_file=path_files ,
                            file_name=opt_files ,
                            product="options" ,
                            num_orders=num_orders ,
                            chunksize=chunksize ,
                            read_ram=read_ram)
        opt_name = path_out + opt_files[0][:-5] + "OPTIONS"
        opt_times = options['sending_time']
        __np__.save(file=opt_name , arr=options)
        del options
        print("[DONE] -- " + str(key).zfill(3) + " -- " + opt_files[0][:-5] + "OPTIONS")
        fut_file = fixfiles['futures'][key]
        futures = booktable(path_file=path_files ,
                            file_name=fut_file ,
                            product="futures" ,
                            num_orders=num_orders ,
                            chunksize=chunksize ,
                            read_ram=read_ram)
        fut_name = path_out + fut_file[0]
        fut_times = futures['sending_time']
        __np__.save(file=fut_name , arr=futures)
        del futures
        print("[DONE] -- " + str(key).zfill(3) + " -- " + fut_file[0] + "-FUTURES")
        time_file = path_times + fut_file[0]
        times = timetable(fut_times=fut_times , opt_times=opt_times , chunksize=chunksize)
        __np__.save(file=time_file , arr=times)
        print("[DONE] -- " + str(key).zfill(3) + " -- " + time_file + "-TIMES")
        del times


# TODO: look into FixDict class on fixfast.py make it more robust
def booktable( path_file=None ,
               file_name=None ,
               product=None ,
               num_orders=1 ,
               chunksize=25600 ,
               read_ram=True ,
               dtype=None ):
    """
    Function to convert fix books to pandas dataframe
    :param path_file: Location of FIX order books
    :param file_name: Location for the pandas order books
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
    path_file = [item + "/" if item[-1] != "/" else item for item in [path_file]][0]
    dfs = []
    for item in iter(file_name):
        file_path = path_file + item
        if getsize(file_path) == 0:
            continue
        else:
            # FixStruct class from FIX to dictionary
            book_struct = FixStruct(num_orders=num_orders)
            fixdata = open_fix(file_path , compression=False)
            if read_ram:
                data = fixdata.data.readlines()
            else:
                data = fixdata.data
            with __mp__.Pool() as pool:
                rows = pool.map(book_struct.limit_orderbook , data , chunksize=chunksize)
                dfs.extend(rows)
            try:
                data.close()
            except AttributeError:
                pass
    try:
        if dtype is None:
            dtype = {'names': ['bid_level' , 'bid_price' , 'bid_size' , 'msg_seq_num' , 'offer_level' , 'offer_price' ,
                               'offer_size' , 'security_desc' , 'security_id' , 'sending_time' , 'trade_date'] ,
                     'formats': ['>i2' , '>f4' , '>f4' , '>i4' , '>i2' , '>f4' , '>f4' , '|U25' , '|U25' , '>i8' ,
                                 '>i4']}
        book = __np__.array(dfs , dtype=dtype)
    except ValueError:
        book = __np__.array([] , dtype=dtype)
    return book


"""
# TODO: look into FixDict class on fixfast.py make it more robust
def booktable( path=None ,
               path_out=None ,
               file_name=None ,
               product=None ,
               num_orders=1 ,
               chunksize=25600 ,
               read_ram=True ,
               dtype=None ):
    if product is None:
        raise ValueError("Product cant be None. Types: futures or options")
    elif product not in "opt|options" and product not in "fut|futures":
        raise ValueError("Product Types: futures or options")
    path = str([item + "/" if item[-1] != "/" else item for item in [path]][0])
    dfs = []
    for item in iter(file_name):
        file_path = path + item
        if getsize(file_path) == 0:
            continue
        else:
            # FixStruct class from FIX to dictionary
            book_struct = FixStruct(num_orders=num_orders)
            fixdata = open_fix(file_path , compression=False)
            if read_ram:
                data = fixdata.data.readlines()
            else:
                data = fixdata.data
            with __mp__.Pool() as pool:
                rows = pool.map(book_struct.limit_orderbook , data , chunksize=chunksize)
                dfs.extend(rows)
            try:
                data.close()
            except AttributeError:
                pass
    try:
        if dtype is None:
            dtype = {'names': ['bid_level' , 'bid_price' , 'bid_size' , 'msg_seq_num' , 'offer_level' , 'offer_price' ,
                               'offer_size' , 'security_desc' , 'security_id' , 'sending_time' , 'trade_date'] ,
                     'formats': ['>i2' , '>f4' , '>f4' , '>i4' , '>i2' , '>f4' , '>f4' , '|U25' , '|U25' , '>i8' ,
                                 '>i4']}
        book = __np__.array(dfs, dtype=dtype)
        #book = __pd__.DataFrame(dfs ,
        #                        columns=['security_id' , 'security_desc' , 'bid_level' , 'bid_price' , 'bid_size' ,
        #                                 'offer_level' , 'offer_price' , 'offer_size' , 'msg_seq_num' , 'trade_date' ,
        #                                 'sending_time'])
        #book = book.replace('NA' , __pd__.np.nan)
        #book = book.replace('' , __pd__.np.nan)
    except ValueError:
        #book = __pd__.DataFrame()
        book = __np__.array([], dtype=dtype)
    if path_out:
        path_out = str([item + "/" if item[-1] != "/" else item for item in [path_out]][0])
        if product in "opt|options":
            file_name = path_out + file_name[0][:-5] + "OPTIONS"
        elif product in "fut|futures":
            file_name = path_out + file_name[0]
        __np__.save(file=file_name, arr=book)
        #book.to_csv(file_name , index=False)
    return book

"""
