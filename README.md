#dltp

Delta-pack (or expand) an XML dump from MediaWiki, using past dumps as a reference. Or, with -cut and -merge options, just combine dumps or cut info you don't need out of them to get something smaller. Get binaries from 2013-08-12 here:

| arch  | linux | mac | windows |
|-|-|-|-|
| amd64 | [dltp][1] | [dltp.mac][3] | [dltp.exe][4] |
| i386  | [dltp386][2] | n/a | [dltp386.exe][5] |

[1]: http://www.rfarmer.net/dltp/bin/dltp
[2]: http://www.rfarmer.net/dltp/bin/dltp386
[3]: http://www.rfarmer.net/dltp/bin/dltp.mac
[4]: http://www.rfarmer.net/dltp/bin/dltp.exe
[5]: http://www.rfarmer.net/dltp/bin/dltp386.exe

##Packing and unpacking

> dltp [-c] foo.dltp.bz2

Unpacks a .dltp.bz2 (or .dltp.gz, etc.) file. The old XML dump(s) referenced by the delta need to exist in the same directory. `-c` forces output to stdout.

You may have trouble if the old XML dumps don't exist or are truncated or have otherwise been processed. The error message may or may not make clear that this is the problem, depending on just where things went wrong.

It works to pipe a .dltp file (uncompressed) to stdin; then the program looks for reference file(s) in the current directory and send XML to stdout by default. `-f` redirects that output to a file (which is named automatically) in the current directory.

> dltp new.xml reference1.xml [reference2.xml...]

Packs a new MediaWiki XML dump using the old file(s) as reference. If you have multiple reference files (like several days of adds-changes dumps), list the newest file first.

##Secondary compression with bzip, etc.

On Linux, compression and decompression of both XML and .dltp files happen by piping through some (de)compression utility you have installed. You can speed up bzip2 (de)compression by installing lbzip2 (which uses multiple cores), and you can store your source files as .lzo (install lzop) or .gz for faster reading. 

On Windows, piping isn't currently possible and (de)compression goes at less than native speed. You may want to unpack files with native tools before feeding them to dltp.

When packing, the -zip option lets you choose an output compression format (none, lzo, gz, bz2, or xz). The default is 'auto', which means .bz2 on Linux as long as a bzip2 binary is in the PATH, and .gz otherwise.

##Cutting and merging

> dltp -cut [-lastrev] [-ns 0] [-cutmeta] < dump.xml

Rather than packing or unpacking, cuts down a MediaWiki export by skipping all but the last revision in each page's history, skipping out pages outside a given namespace, and/or skipping contributor info and revision comments. Always streams XML from stdin to stdout.

You can also use the `-lastrev`, etc. flags while packing, if you want. The advantage to cutting down the source in a separate step is that you end up with a raw file you can use as a reference file for future diffs, post online as a standalone download, get an md5sum of, etc.

To save memory, right now you should usually cut adds-changes dumps down with `-lastrev`; otherwise we hold  page's whole history in memory at once, which can be a problem for big, very active pages (e.g., admin noticeboards).

> dltp -merge file1.xml file2.xml [file3.xml...]

Merges a set of files to stdout. For a given page ID, the version from the leftmost file on the command line takes precedence. You could use this to create something like a weekly dump out of a set of daily dumps, or to create something like an all-pages dump from an earlier all-pages dump plus adds-changes dumps. These wouldn't represent the wiki's latest content perfectly, though, because adds-changes dumps don't cover deletion or oversighting.

You may pass `-merge` any of the options `-cut` accepts. Again, using at least `-lastrev` is a good idea to save memory when dealing with adds-changes dumps.

##Passing URLs on the command line

If you're feeling daring, try something experimental and pass http:// (but not https://) URLs on the command line instead of files. Note that the whole file is saved to disk (so you still need the disk space) and there's no way to resume an interrupted download. If you specify multiple URLs, they'll download parallel; you might hit a server-imposed limit if you try to download more than two files at once.

If you have trouble using dltp with network resources, download the files manually, try again with the local files, and see whether that solves it.

##Debugging and caveats

Running with `-debug` will make the program print more detail after a fatal error, which includes not only bugs but mundane things like missing files, network trouble, or a keyboard interrupt.

This is not stable, heavily tested software. It has no warranty, and breaking changes to the format will happen.  I'd love to know if you're interested in using or working on it.

Public domain, 2013; no warranty.
