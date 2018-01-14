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
        data_date = datetime.datetime(year=int(self.data_start_date[:4]),
                                  month=int(self.data_start_date[4:6]),
                                  day=int(self.data_start_date[6:8]))
        start_date = data_date - datetime.timedelta(days=31)
        end_date = start_date + datetime.timedelta(days=365)
        data_start = {start_date.year, start_date.month}

        if not os.path.exists(str(self.data_out)):
            with self.output().open('w') as out:
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
                                out.write("%s\n" % os.path.join(root, name))

    def output(self):
        name = self.data_start_date[:4] + "-" + str(self.file_name)
        target = luigi.LocalTarget(self.data_out + name)
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
        if not os.path.exists(self.data_out):
            os.makedirs(self.data_out)
        for month in self.data_months.split(","):
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
    data_path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.data_path)


class OrderBooks(luigi.Task):
    data_pipe = luigi.Parameter()
    data_year = luigi.Parameter()
    year_code = luigi.Parameter()
    chunksize = luigi.IntParameter(default=31000)   # 10**5
    src_name = luigi.Parameter(default="files.txt")
    filename = luigi.Parameter(default="books.txt")
    data_out = ""

    def requires(self):
        self.data_pipe = str([item + "/" if item[-1] != "/" else item for item in [self.data_pipe]][0])
        files = self.data_year + "-" + self.src_name
        return [FixFiles(self.data_pipe + files)]

    def run(self):
        self.data_out = self.data_pipe + self.data_year + "/"
        with self.input()[0].open('r') as infile:
            files = infile.read().splitlines()
        contracts = self.output().open('w')

        for k, file in enumerate(files):
            fixdata = fx.open_fix(path=file.strip())
            dates = fixdata.dates
            securities = fx.liquid_securities(fixdata, year_code=self.year_code)

            opt_code = fx.most_liquid(dates=dates,
                                      instrument="ES",
                                      product="OPT",
                                      year_code=self.year_code)

            fut_code = fx.most_liquid(dates=dates,
                                      instrument="ES",
                                      product="FUT",
                                      year_code=self.year_code)

            desc_path = self.data_out + fut_code[2] + "/"
            filename = str(k).zfill(3) + "-" + fut_code[2] + opt_code[2] + "-"
            path = desc_path + filename

            fx.build_books(fixdata, securities, file_out=True, path_out=path, chunksize=self.chunksize)

            for sec_desc in securities.values():
                name = path + sec_desc.replace(" ", "-")
                contracts.write("%s\n" % name)

            if k % 10 == 0:
                self.set_status_message("Progress: %d / 100" % k)
                # displays a progress bar in the scheduler UI
                self.set_progress_percentage(k)

        contracts.close()

    def output(self):
        return luigi.LocalTarget(self.data_pipe + self.data_year + "-" + self.filename)


if __name__ == "__main__":
    luigi.run()
    #luigi.run(main_task_cls=CMEPipeline)

