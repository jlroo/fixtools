#!/usr/bin/env python3

"""
Created on Tue Oct  10 16:14:23 2017

@author: jlroo

"""

import os
import luigi
import re
import datetime
import fixtools as fx


class FindFiles(luigi.Task):

    data_in = luigi.Parameter()
    data_out = luigi.Parameter()
    data_start_date = luigi.Parameter()
    file_name = luigi.Parameter(default="files.txt")

    def run(self):
        data_start_date = str(self.data_start_date)
        data_date = datetime.datetime(year=int(data_start_date[:4]),
                                  month=int(data_start_date[4:6]),
                                  day=int(data_start_date[6:8]))
        start_date = data_date - datetime.timedelta(days=31)
        end_date = start_date + datetime.timedelta(days=365)
        data_start = {start_date.year, start_date.month}
        out_files = []
        if not os.path.exists(str(self.data_out)):
            for root, dirs, files in os.walk(str(self.data_in)):
                for name in files:
                    file_date = re.findall(r"\d+", name)
                    if file_date:
                        file_date = str(file_date[0])
                        file_date = datetime.datetime(year=int(file_date[:4]),
                                                      month=int(file_date[4:6]),
                                                      day=int(file_date[6:8]))
                        year = file_date.year
                        month = file_date.month
                        if year in data_start and month in data_start or year == end_date.year:
                            out_files.append(os.path.join(root, name))        
            with self.output().open('w') as out:
                out_files.sort()
                out.write('\n'.join(out_files))

    def output(self):
        name = str(self.data_start_date)[:4] + "-" + str(self.file_name)
        name = self.data_out + name
        target = luigi.LocalTarget(name)
        return target


class CMEPipeline(luigi.Task):
    data_in = luigi.Parameter()
    data_out = luigi.Parameter()
    data_start_date = luigi.Parameter()
    data_months = luigi.Parameter()
    filename = luigi.Parameter(default="folders.txt")
    year = ""

    def requires(self):
        self.data_in = str([item + "/" if item[-1] != "/" else item for item in [self.data_in]][0])
        self.data_out = str([item + "/" if item[-1] != "/" else item for item in [self.data_out]][0])
        return FindFiles(self.data_in, self.data_out, str(self.data_start_date))

    def run(self):
        self.year = str(self.data_start_date)[0:4]
        data_months = str(self.data_months).split(",")
        if not os.path.exists(self.data_out):
            src, dirs, files = next(os.walk(self.data_out))
            src_path = "/".join(src.split("/")[:-2])
            dirs = [src_path + i for i in ["/output/","/parity/","/times/","/rates/"]]
            for folder in dirs:
                os.makedirs(folder)
            os.makedirs(src)
        for month in data_months:
            if not os.path.exists(self.data_out + self.year + "/" + month):
                os.makedirs(self.data_out + self.year + "/" + month)

        with self.output().open('w') as out:
            for root, dirs, files in os.walk(self.data_out + self.year):
                for fn in [os.path.join(root, name) for name in dirs]:
                    out.write("%s\n" % fn)

    def output(self):
        target = luigi.LocalTarget(self.data_out + self.year + "-" + str(self.filename))
        return target


class FixFiles(luigi.Task):
    data_path = str(luigi.Parameter())

    def output(self):
        return luigi.LocalTarget(self.data_path)


class OrderBooks(luigi.Task):
    data_pipe = luigi.Parameter()
    data_year = str(luigi.Parameter())
    year_code = luigi.Parameter()
    processes = luigi.IntParameter()
    compression = luigi.BoolParameter(default=True)
    chunksize_filter = luigi.IntParameter(default=25600)
    chunksize_book = luigi.IntParameter(default=10)
    src_name = str(luigi.Parameter(default="files.txt"))
    filename = luigi.Parameter(default="books.txt")
    data_out = luigi.Parameter(default="")

    def requires(self):
        self.data_pipe = str([item + "/" if item[-1] != "/" else item for item in [self.data_pipe]][0])
        files = self.data_year + "-" + self.src_name
        return [FixFiles(self.data_pipe + files)]

    def run(self):
        if self.data_out == "":
            self.data_out = self.data_pipe + self.data_year + "/"
        with self.input()[0].open('r') as data_files:
            files = sorted(data_files.read().splitlines())
        contracts = self.output().open('w')
        for k, infile in enumerate(files):
            fixdata = fx.open_fix(path=infile.strip() , compression=self.compression)
            data_lines = []
            for n , line in enumerate(fixdata.data):
                if n >= 10000:
                    break
                data_lines.append(line)
            fixdata.data.seek(0)
            opt_code = fx.most_liquid(data_line=data_lines[0] , product="ES" , instrument="OPT" ,
                                      code_year=self.year_code)
            fut_code = fx.most_liquid(data_line=data_lines[0] , product="ES" , instrument="FUT" ,
                                      code_year=self.year_code)
            liquid_secs = fx.liquid_securities(data_lines, code_year=self.year_code)
            desc_path = self.data_out + fut_code[2] + "/"
            filename = str(k).zfill(3) + "-" + fut_code[2] + opt_code[2] + "-"
            path_out = desc_path + filename
            fx.data_book(data=fixdata.data ,
                         securities=liquid_secs ,
                         path=path_out ,
                         processes=self.processes ,
                         chunksize_filter=self.chunksize_filter ,
                         chunksize_book=self.chunksize_book)
            for secid in liquid_secs.keys():
                name = path_out + liquid_secs[secid].replace(" ", "-")
                contracts.write("[DONE] " + infile.strip() + " -- CONTRACT -- " + name + "\n")
                print("[DONE] " + infile.strip() + " -- CONTRACT -- " + name)
        contracts.close()

    def output(self):
        return luigi.LocalTarget(self.data_pipe + self.data_year + "-" + self.filename)


if __name__ == "__main__":
    luigi.run()
    # luigi.run(main_task_cls=CMEPipeline)
