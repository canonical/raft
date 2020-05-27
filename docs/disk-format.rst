Disk format
===========

The implementation of metadata and log persistency is virtually the same as the
one found in  `LogCabin`_.

The disk files consist of metadata files, closed segments, and open
segments. Metadata files are used to track Raft metadata, such as the server's
current term, vote, and log's start index. Segments contain contiguous entries
that are part of the log. Closed segments are never written to again (but may be
renamed and truncated if a suffix of the log is truncated). Open segments are
where newly appended entries go. Once an open segment reaches the maximum
allowed size, it is closed and a new one is used. There are usually about 3 open
segments at any given time, the one with the lower index is the one actively
being written, and the other ones have been fallocate'd and are ready to be used
as soon as the active one gets closed.

Metadata files are named "metadata1" and "metadata2". The code alternates
between these so that there is always at least one readable metadata file.

On startup, the readable metadata file with the higher version number is used.

The format of a metadata file is:

* [8 bytes] Format (currently 1).
* [8 bytes] Incremental version number.
* [8 bytes] Current term.
* [8 bytes] ID of server we voted for.

All values are in little endian encoding.

Closed segments are named by the format string "%lu-%lu" with their start and
end indexes, both inclusive. Closed segments always contain at least one entry,
and the end index is always at least as large as the start index. Closed segment
files may occasionally include data past their filename's end index (these are
ignored but a warning is logged). This can happen if the suffix of the segment
is truncated and a crash occurs at an inopportune time (the segment file is
first renamed, then truncated, and a crash occurs in between).

Open segments are named by the format string "open-%lu" with a unique
number. These should not exist when the server shuts down cleanly, but they
exist while the server is running and may be left around during a crash.  Open
segments either contain entries which come after the last closed segment or are
full of zeros. When the server crashes while appending to an open segment, the
end of that file may be corrupt. We can't distinguish between a corrupt file and
a partially written entry. The code assumes it's a partially written entry, logs
a warning, and ignores it.

Truncating a suffix of the log will remove all entries that are no longer part
of the log. Truncating a prefix of the log will only remove complete segments
that are before the new log start index. For example, if a segment has entries
10 through 20 and the prefix of the log is truncated to start at entry 15, that
entire segment will be retained.

Each segment file starts with a segment header, which currently contains just an
8-byte version number for the format of that segment. The current format
(version 1) is just a concatenation of serialized entry batches.

Each batch has the following format:

* [4 bytes] CRC32 checksum of the batch header, little endian.
* [4 bytes] CRC32 checksum of the batch data, little endian.
* [  ...  ] Batch of one or more entries.

.. _LogCabin: https://github.com/logcabin/logcabin/blob/master/Storage/SegmentedLog.h
