#dltp

Delta-pack (or expand) an XML dump from MediaWiki, using past dumps as a reference. 

> dltp [-c] foo.dltp.bz2

Unpacks a .dltp.bz2 (or .dltp.gz, etc.) file. The old XML dump(s) referenced by the delta need to exist in the same directory, and it's OK if they're compressed with bzip2 or gzip (or lzop or xz, if we can pipe through them). `-c` forces output to stdout.

It works to pipe a .dltp file (uncompressed) to stdin; then the program looks for reference file(s) in the current directory and send XML to stdout by default. `-f` redirects that output to a file (which is named automatically; don't put a name on the command line) in the current directory.

On Windows, bzip2 decompression is in Go and a bit slower, so you may want to store files gzipped or uncompressed. On Unix, installing lbzip2 will speed decompression by using multiple cores.

> dltp new.xml reference1.xml [reference2.xml...]

Packs a new MediaWiki XML dump using the old file(s) as reference. If you have multiple reference files (like several days of adds-changes dumps), list the newest file first.

Output is piped through lbzip2 or bzip2 unless you request otherwise: `-z` forces gzip and `-r` disables compression entirely. Input files may be compressed.

On Windows, .dltp.gz is produced by default instead of .dltp.bz2, because there is no pure-Go bz2 packer.

> dltp -cut [-lastrev] [-ns 0] [-cutmeta] < dump.xml

Rather than packing or unpacking, cuts down a MediaWiki export by skipping all but the last revision in each page's history, skipping out pages outside a given namespace, or skipping contributor info and revision comments. Always streams XML from stdin to stdout.

You can also use the -lastrev, etc. flags while packing, if you want. The advantage to cutting down the source in a separate step is that you end up with a raw file you can use as a reference file for future diffs, post online as a standalone download, get an md5sum of, etc.

Cutting using -lastrev is a good idea before compressing adds-changes dumps: dltp needs to store a whole page's history in RAM when packing or unpacking, and many revisions of a big, active page can use a lot of it.

> dltp -merge file1.xml file2.xml [file3.xml...]

Merges a set of files to stdout. For a given page ID, the version from the leftmost file on the command line takes precedence. You could use this to create something like a weekly dump out of a set of daily dumps, or to create something like an all-pages dump from an earlier all-pages dump plus adds-changes dumps. (These wouldn't be represent the wiki's current content perfectly, though, because adds-changes dumps don't cover deletion or oversighting.)

You may pass `-merge` any of the options `-cut` accepts.

## Caveats
This is not stable, heavily tested software. It has no warranty, and breaking changes to the format will happen.  I'd love to know if you're interested in using or working on it, though.

Public domain, 2013; no warranty.