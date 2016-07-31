# fixtools

This tool kit was created to make it easier to work and analyze FIX/FAST - FIX 5.0 SP2 financial data from the CME group. Some of its features will help you identify most trdaded securities ( futures,options ), break large week FIX bianary files into its corresponding trading days by reading a gzip files containing the FIX file or a binary FIX data file also you can create an order book for a giving security. Finally in conjuction with the nodejs fix2json package from SunGard-Labs you can insert the processed FIX records into JSON-conformant NoSQL repositories, such as MongoDB.

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
    fx.group_by(fixdata,security_id)

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
