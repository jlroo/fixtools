# fixtools

Overview
---------
This tool kit was created to make it easier to work and analyze FIX 5.0 SP2 financial data from the CME group. Some of its features will help you identify most traded securities ( futures,options), break large week FIX binary files into its corresponding trading days. You can read in gzip files containing FIX data or uncompress files also you can create an order book for a giving security. Finally in conjunction with the nodejs fix2json package from SunGard-Labs you can insert the processed FIX records into JSON-conformant NoSQL repositories, such as MongoDB.

Background
----------
This tool kit was developed to help us work with the raw FIX data, analyze it and efficiently identify key components without having to spend too much time setting up FIX engines/applications to parse and analyze the data. The data use on this project is CME Market Depth FIX files - E-mini S&P 500. These files provide all market data messages required to recreate the order book. These files are an important part in a research that aims to determine mispricing in the [E-mini S&P 500](http://www.cmegroup.com/trading/equity-index/us-index/e-mini-sandp500.html). 

FIX data format layout
--------------------------
Messages fields are delimited by the ASCII <start of header> character in binary (000 0001), hex (\x01) or the caret (^A) notation often used to represent control characters on a terminal/text editor. Fix messages are composed of a header, a body, and a tail.<br>

For the FIX 5.0 SP2 protocol, the header contains standard mandatory fields and some optional field that are placed in a predetermine order, for example: 8 (BeginString), 9 (BodyLength), 35 (MsgType), 49 (SenderCompID), 56 (TargetCompID) and 1128 (ApplVerID). <br>

In the header of the FIX message the (tag 35, MsgType) message type at the beginning of the message. The last field of the FIX message is tag 10, which gives the Checksum as a three-digit number (e.g. 10=002). Hence Header+Body+Tail give us the FIX message content.

*Example of a FIX message as String object:*

    1128=8^A9=147^A35=X^A49=CME^A34=5204^A52=20090104185930700^A75=20090105^A268=1^A279=0^A22=8^A48=9323^A83=1^A107=ESH0^A269=0^A270=65000^A271=2^A273=185930000^A336=2^A346=1^A1023=1^A10=148^A

*String encode as bytes object*

    b'1128=9\x019=431\x0135=d\x0149=CME\x0134=334\x0152=20130106170100030\x0115=USD\x0122=8\x0148=382206\x0155=ES\x01107=ESH4\x01200=201403\x01202=0\x01207=XCME\x01461=FFIXSX\x01462=5\x01562=1\x01731=1\x01827=2\x01864=2\x01865=5\x01866=20121221\x011145=143000000\x01865=7\x01866=20140321\x011145=133000000\x01870=3\x01871=24\x01872=1\x01871=24\x01872=4\x01871=24\x01872=14\x01947=USD\x01969=25\x01996=IPNT\x011140=2000\x011141=1\x011022=GBX\x01264=10\x011142=F\x011143=600\x011146=12.5\x011147=50\x011148=136350\x011149=150350\x011150=143125\x011151=ES\x011180=7\x015796=20130104\x019787=0.01\x019850=0\x0110=018\x01\n'

Installation
------------

    $ python setup.py install

Examples
------------

Weekly FIX files to daily files

    > import fixtools as fx
    > path_gzip =  "XCME_ES_20130103_20130111.gz"
    > days = fx.periods(path_gzip)
    > fx.to_day(path_gzip,days)

Number of contracts and volume

    > import fixtools as fx
    > path_gzip =  "XCME_ES_20130103_20130111.gz"
    > records = fx.contracts(path_gzip)
    > records = records.report

Group by security ID number

    > import fixtools as fx
    > path_gzip =  "XCME_ES_20130103_20130111.gz"
    > security_id = "222858"
    > fx.group_by(path_gzip,security_id)
    
Build Books

    > path = "/home/jlroo/data/2010/XCME_MD_ES_20100104_20100108.gz"
    > fixdata = fx.open_fix(path)
    > securities = fx.liquid_securities(fixdata)
    > fx.build_books(fixdata, securities, chunksize = 10**5)

Pipelines

    > python -m luigi --module pipelines CMEPipeline --local-scheduler --data-in "/Users/jlroo/cme/data/2010" --data-out "/Users/jlroo/cme/pipeline" --data-start-date 20100101 --data-months 'H,M,U,Z'
    > python -m luigi --module pipelines OrderBooks --local-scheduler --data-pipe "/home/cme/2010/pipeline" --compression --chunksize-filter 25600 --processes 40 --data-year 2010 --year-code 0

Book Search
    
    > books_locator(exchange='XCME', 
                    year=2010,
                    asset='EQUITY',
                    product='ES',
                    month=2,
                    instrument='FUT',
                    local_path='/home/cme/',
                    top_books=True)


See Also
------------

* [FIX Protocol](http://fixprotocol.org)
* [FixSpec.com Developer Tools](https://fixspec.com/developers)
* [FIX on Wikipedia](http://en.wikipedia.org/wiki/Financial_Information_eXchange)
* [fix2json](https://github.com/SunGard-Labs/fix2json)
* [CME DataMine](http://www.cmegroup.com/market-data/datamine-historical-data.html)

License
----------

**fixtools** © 2022, St. Louis, Missouri.<br> 
Released under the [MIT License].<br>
Authored and maintained by Jose Luis Rodriguez.

> LinkedIn [jlroo](https://www.linkedin.com/in/jlr) &nbsp;&middot;&nbsp;
> GitHub [@jlroo](https://github.com/jlrop) &nbsp;&middot;&nbsp;

[MIT License]: http://mit-license.org/
[contributors]: http://github.com/jlroo
