#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Created on Sun Nov  5 09:53:03 2017

@author: jlroo

python run-book.py --file "/work/05191/jlroo/stampede2/2010/XCME_MD_ES_20091207_2009121" --year_code "0" 
--data_out "/scratch/05191/jlroo/data" --chunksize 32
"""

import fixtools as fx
import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--file' , dest='file_path' , help='Fix data file input')
    parser.add_argument('--path_out' , dest='path_out' , help='Fix data file output')
    parser.add_argument('--year_code' , dest='year_code' , help='Fix data year code')
    parser.add_argument('--data_out' , dest='data_out' , help='Fix books path out')
    parser.add_argument('--compression', dest='compression', action='store_true')
    parser.add_argument('--no-compression', dest='compression', action='store_false')
    parser.add_argument('--process' , dest='process' , help='Number of threads')
    parser.add_argument('--book_process' , dest='book_process' , help='Number of threads')
    parser.add_argument('--chunksize_filter' , dest='chunksize_filter' , help='data chunksize')
    parser.add_argument('--chunksize_book' , dest='chunksize_book' , help='data chunksize')
    parser.set_defaults(compression=True)
    args = parser.parse_args()

    fixdata = fx.open_fix(path=args.file_path , compression=args.compression)
    data_lines = []
    for n,line in enumerate(fixdata.data):
        if n >= 10000:
            break
        data_lines.append(line)
    fixdata.data.seek(0)
    opt_code = fx.most_liquid(data_line=data_lines[0], instrument="ES", product="OPT", code_year=args.year_code)
    fut_code = fx.most_liquid(data_line=data_lines[0], instrument="ES", product="FUT", code_year=args.year_code)
    liquid_secs = fx.liquid_securities(data_lines, code_year=args.year_code)
    desc_path = args.data_out + fut_code[2] + "/"
    filename = str(0).zfill(3) + "-" + fut_code[2] + opt_code[2] + "-"
    path_out = desc_path + filename

    fx.data_book(data=fixdata.data , securities=liquid_secs ,
                 path=path_out , processes=args.processes ,
                 chunksize_filter=args.chunksize_filter ,
                 chunksize_book=args.chunksize_book)


if __name__ == '__main__':
    main()
