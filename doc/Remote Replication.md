# Packing to a remote archive

The most efficient way to do this would be to transfer at least part
of the index to the client end, so it could do the bulk of the dedup
checking.  But this has serious security flaws; a bad actor could deliberately
say their stream contained known hashes and then get access to this data
when they replayed their stream.  We could transfer the bloom filter alone,
but I'd need convincing that there wasn't a security issue, and I'm not sure
it would save much traffic in the end.

So the client will be responsible for:

Packing:

- splitting the input stream into chunks
- calculating the hash
- sending the hash to the archive/server
- the archive server will respond with whether they've seen that hash before
- the client will send any unseen chunks to the server.

Unpacking:

- Server sends stream of chunks
- client reassembles


For performance reasons we need to send batches of hashes to the server, and in turn the
server needs to reply with batches of responses.  Do we want a separate connection for each
of these channels?

We should allow both send and response batches to be out of order.  This allows us to use
multiple threads at either end.

Compression should be implemented at the batch level.


# Implementation

I suggest we start by introducing client/server interfaces that are based around
std::sync::mpsc::sync_channel.  One channel for hashes, one channel for hash responses,
one channel for chunks and a control channel?  The channels will not be batched; when
doing non-local ops the channels will have a thread at the other end that handles batching
and compression.
