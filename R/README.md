# R Vertica UDx Examples
This directory contains example code for use with Vertica R User-Defined Extensions (UDx).

For more information on using R UDxs, see the [Vertica documentation].

## Running the examples
Clone the repo where your Vertica database is installed, and from the cloned repo run:

```
$ vsql -f <sql_file_included_with_code_example>
```

If the command generates an error, then you might need to adjust the path used in the example SQL file.


[Vertica documentation]: https://my.vertica.com/docs/latest/HTML/index.htm#Authoring/ExtendingVertica/R/DevelopingWithTheRSDK.htm
