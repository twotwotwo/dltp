dltp
====

Packs differences between two MediaWiki dump files for quicker download. 
The name comes from "delta pack," inelegantly squashed.

> dltp newer.xml older.xml

creates newer.xml.dltp.bz2 (or .gz).

> dltp newer.xml.dltp.bz2

tries to create a newer.xml from an older.xml in the current directory. 

If newer.xml or older.xml is not found, dltp will look for compressed
versions of them.  It will look for (l)bzip2, gzip/pigz, or xz utilities in
the PATH to pipe through for decompression.  Output is also compressed by
default, using bzip2 or gzip.  If compression programs aren't found in the
PATH, dltp will still read and write .gz and read bzip2 with golang's
built-in libraries.

dltp -help shows flags: briefly, -z, -j, -x, and -n request gzip, bzip2, xz,
and no output compression.  When unpacking, -c forces output to stdout.

You can pipe a .dltp file to stdin when unpacking. In this case, no
automatic decompression happens, so you may need to manually pipe through
gunzip or whatever.  When piping, dltp will look in the working directory
for source files, and output will go to stdout; you can force it to a file
in the working directory with the -f flag.

This is not stable software; it might not work for you, and breaking changes
to the format could happen.  If you're interested anyway, be careful and
compare the unpacked file hash to the original, and know that issues,
comments, pull requests, etc. are welcome.

Public domain, 2013; no warranty.