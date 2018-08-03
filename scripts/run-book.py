#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Created on Sun Nov  5 09:53:03 2017

@author: jlroo

python run-book.py --file "/work/05191/jlroo/stampede2/2010/XCME_MD_ES_20091207_2009121" --year_code "0" --data_out "/scratch/05191/jlroo/data" --chunksize 38000
"""

import fixtools as fx
import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--file' , dest='file_path' , help='Fix data file input')
    parser.add_argument('--year_code' , dest='year_code' , help='Fix data year code')
    parser.add_argument('--data_out' , dest='data_out' , help='Fix books path out')
    parser.add_argument('--compression' , dest='compression' , default=False ,
                        type=lambda x: (str(x).lower() == 'true') , help='True/False flag compression')
    parser.add_argument('--chunksize' , dest='chunksize' , help='Data chunksize')

    args = parser.parse_args()

    fixdata = fx.open_fix(path=args.file_path , compression=args.compression)
    data_lines = fixdata.data.readlines(10000)
    fixdata.data.seek(0)
    opt_code = fx.most_liquid(data_line=data_lines[0] , instrument="ES" , product="OPT" , code_year=args.year_code)
    fut_code = fx.most_liquid(data_line=data_lines[0] , instrument="ES" , product="FUT" , code_year=args.year_code)
    liquid_secs = fx.liquid_securities(data_lines , code_year=args.year_code)

    filename = fut_code[2] + opt_code[2] + "-"
    path_out = args.data_out + filename
    fx.data_book(data=fixdata.data , securities=liquid_secs , path=path_out , chunksize=int(args.chunksize))


if __name__ == '__main__':
    main()
