# fixtools

Overview
---------
This tool kit was created to make it easier to work and analyze FIX 5.0 SP2 financial data from the CME group. Some of its features will help you identify most trdaded securities ( futures,options ), break large week FIX bianary files into its corresponding trading days. You can read in gzip files containing FIX data or uncompress binary files also you can create an order book for a giving security. Finally in conjuction with the nodejs fix2json package from SunGard-Labs you can insert the processed FIX records into JSON-conformant NoSQL repositories, such as MongoDB.

Background
----------
The Quinlan School of Business at Loyola University Chicago acquired a couple of years CME Market Depth FIX files - E-mini S&P 500. These files provide all market data messages required to recreate the order book. These files are an important part in a research that aims to determien mispricing in the [http://www.cmegroup.com/trading/equity-index/us-index/e-mini-sandp500.html](E-mini S&P 500). This tool kit was developed to help us work with the raw binary data, analyze it and efficienly identify key components without having to spend too much time setting up FIX engines/applications to parse and analyze the data.

FIX data format layout
--------------------------
Messages fields are delimited by the ASCII <start of header> character in binary (000 0001), hex (\x01) or the caret (^A) notation often used to represent control characters on a terminal/text editor. Fix messages are composed of a header, a body, and a footer/tail.<br>

For the FIX 5.0 SP2 protocol, the header contains standard mandatory fields and some optional field that are placed in a predetermine order, for example: 8 (BeginString), 9 (BodyLength), 35 (MsgType), 49 (SenderCompID), 56 (TargetCompID) and 1128 (ApplVerID). <br>

In the header of the FIX message the (tag 35, MsgType) message type at teh beggining of the message. The last field of the FIX message is tag 10, which gives the Checksum as a three-digit number (e.g. 10=002).

    Header+Body+Trailer : FIX Content

*Example of a FIX message :*

    1128=89=14735=X49=CME34=520452=2009010418593070075=20090105268=1279=022=848=932383=1107=ESH0269=0270=65000271=2273=185930000336=2346=11023=110=148

Installation
------------

    $ python setup.py install


Examples
------------

Weekly FIX files to daily files

    > import fixtools as fx
    > path_gzip =  "XCME_ES_20130103_20130111.gz"
    > days = fx.periods(path_gzip)
    > fixdata = fx.read_fix(path)
    > fx.to_day(fixdata,days)

Number of contracts and volume

    > import fixtools as fx
    > path_gzip =  "XCME_ES_20130103_20130111.gz"
    > fixdata = fx.read_fix(path)
    > records = fx.contracts(fixdata)
    > records = records.report

Group by security ID number

    > import fixtools as fx
    > path_gzip =  "XCME_ES_20130103_20130111.gz"
    > fixdata = fx.read_fix(path)
    > security_id = "222858"
    > fx.group_by(fixdata,security_id)

See Also
------------

* [FIX Protocol](http://fixprotocol.org)
* [FixSpec.com Developer Tools](https://fixspec.com/developers)
* [FIX on Wikipedia](http://en.wikipedia.org/wiki/Financial_Information_eXchange)
* [fix2json](https://github.com/SunGard-Labs/fix2json)
* [MongoDB](https://www.mongodb.com/community)

License
----------

**fixtools** © 2016, Chicago, Illinois.<br> 
Released under the [MIT License].<br>
Authored and maintained by Jose Luis Rodriguez.

> [jlroo.com](http://jlroo.com) &nbsp;&middot;&nbsp;
> GitHub [@jl_roo](https://github.com/jl_roo) &nbsp;&middot;&nbsp;
> Twitter [@jl_roo](https://twitter.com/jl_roo)

[MIT License]: http://mit-license.org/
[contributors]: http://github.com/jlroo
