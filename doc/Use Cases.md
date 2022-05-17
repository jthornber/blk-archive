# Create a new archive
An archive is a collection of files stored under a root directory.  All the tools will need to be given this root directory using the -a command switch.

To create a new archive use the *create* sub command with the -a switch.  The directory specified must not exist; create will create it and setup files within.

> blk-archive create -a my-archive

Each archive has a *block-size* associated with it.  When deduplicating data blk-archive will chop the input up into blocks and then see if it's seen that block before.  The *block-size* parameter specifies the _average_ block size for these blocks.

There is an overhead associated with storing every block in the system (~37 bytes).

Smaller block sizes increase the probability of finding duplicate data, but increase the space overhead, and reduce performance slightly.

The default block size is 4096, if in doubt leave it as this.  You probably only want to go lower than this if you are intending to store small amounts of data in the system (eg, < 1T).

> blk-archive create -a my-archive --block-size 8092

The block size will always get rounded up to a power of two.

# Add a large file to an archive
> blk-archive pack -a my-archive my-file

The *pack* sub command adds a file to an archive.  The file will be deduplicated then compressed.  Finally the compressed data will be reconstructed to verify the process.

blk-archive is really designed for archiving data on devices.  It will do files

Various statistics will be printed out at the end of the operation:

> blk-archive pack -a test-achive linux-v5.0.tar

```
stream id        : 30973e06100eb1d7
file size        : 823.31M
mapped size      : 823.31M
fills size       : 20.79K
duplicate data   : 21.16M
data written     : 154.46M
hashes written   : 8.43M
stream written   : 5.16K
compression      : 19.79%
speed            : 125.43M/s
```

- stream id; This is a unique identifier for the packed stream.  You will need this in order to restore this stream later.
- file size; the size of the original, uncompressed file.
- mapped size; if this was a thin device then not all of it may contain data.  In this case it's totally mapped, so the mapped size and file size are identical.
- fills size; how much of the file was filled with repeating bytes.  Such as a run of 1000 zeroes.
- duplicate data;  How much of the data was found to be already in the archive.  In this case we are dealing with an empty archive so the only duplicate data we will find will be from within the file itself.  We find only 27 meg.
- data written; This is how much compressed data was written to the archive.
- hashes written; Each block written has a 256 bit hash associated with it that we use for deduplication.  These hashes can take up considerable space for large archives.
- stream written; This is the metadata for the stream.  Generally it will be very small compared to the data written.
- compression; (mapped size) / (data written + hashes written + stream written)
- speed; how quickly we compressed the file

If we pack a similar file into this archive we should see the deduplication in action:

> blk-archive pack -a test-archive linux-v5.1.tar

```
stream id        : 0acabdfa1dfb98a7
file size        : 831.28M
mapped size      : 831.28M
fills size       : 20.99K
duplicate data   : 408.43M
data written     : 87.50M
hashes written   : 4.06M
stream written   : 44.32K
compression      : 11.02%
speed            : 122.39M/s
```
Note we found 408M of data that was already in the system.  Causing us to write only 87M of data for this file (after compression).


To demonstrate the effect of block size on data deduplication let's repeat the above with a block size of just 256 bytes (only recommended for small archives).

> blk-archive create -a test-archive --block-size 256

> blk-archive pack -a test-archive linux-v5.0.tar

```
stream id        : 25eaa5f7631a1fd5
file size        : 823.31M
mapped size      : 823.31M
fills size       : 2.65M
duplicate data   : 55.47M
data written     : 151.39M
hashes written   : 108.54M
stream written   : 287.63K
compression      : 31.61%
speed            : 76.65M/s
```

We wrote many more hashes, slowed down archiving speed, and achieved worse compression.

But we get a pay off when we try and pack similar data:

>blk-archive pack -a test-archive linux-v5.1.tar

```
stream id        : ac72aceac23c8930
file size        : 831.28M
mapped size      : 831.28M
fills size       : 2.71M
duplicate data   : 745.80M
data written     : 14.09M
hashes written   : 10.52M
stream written   : 506.52K
compression      : 3.02%
speed            : 64.76M/s
 ```

Now we find 745M of duplicate data out of 831M, and compress the input file down to 3% of it's original size.  Notice how the hashes written is taking up almost as much space as the data written.

# Add a non-live block device to an archive
Adding a block device to an archive is identical to adding a file.  Just specify the path to the device.  You will also need permissions to read the block device.

*The block device must not be in use*.  For instance, you cannot archive a block device that contains a mounted filesystem (later recipes will describe how to get round this using lvm snapshots or thin snapshots).

>blk-archive pack -a test-archive /dev/my-vg/my-lv 

# Use lvm old snapshot to archive a live block device
Get input from ZK

# Add a thin device to an archive
Invoking packing of a thin device is identical to packing any other device:

> blk-archive pack -a test-archive /dev/my-vg/my-thin-lv

However under the hood blk-archive is aware that this device is a thin device.  It will therefore take a metadata snapshot of the pool, and scan the pool's metadata to ascertain which parts of the thin device have been provisioned.  Only these provisioned regions are read, potentially saving much time.

```
stream id        : 637daeec766b537d
file size        : 100G
mapped size      : 5.47G
fills size       : 611.72M
duplicate data   : 93.65M
data written     : 3.72G
hashes written   : 51.05M
stream written   : 66.73K
compression      : 68.94%
speed            : 133.31M/s
```
Here I'm packing a 100G block device that contains an ext4 filesystem.  The filesystem just has the linux kernel git repo on it.

You can see that we only packed 5.47G of data rather than the full 100G.  Compression was poor (69%) because most of the data in a git repo is already compressed.

# Use a thin snapshot to archive a live thin device
If you want to archive a thin device but it's in use, you can simply take a thin snapshot:

>lvcreate -s --name v5.1 test-vg/kernel-builds
>lvchange -ay -Ky test-vg/v5.1

The first *lvm* command creates the snapshot, the second activates it.

Now we're going to use the archive from the previous recipe.  Our thin snapshot contains the filesystem with the linux git repo on it.  The only difference from before being it has version v5.1 checked out rather than v5.0.

>blk-archive pack -a test-archive /dev/test-vg/v5.1

```
stream id        : 1cb9b909497af41d
file size        : 100G
mapped size      : 5.47G
fills size       : 588.12M
duplicate data   : 4.85G
data written     : 4.02M
hashes written   : 447.05K
stream written   : 69.93K
compression      : 0.08%
speed            : 134.90M/s
```

Lot's of duplicate data is found so we only wrote ~4.5M of new data to the archive.

Once the pack is complete the snapshot can be deleted:

>lvremove -y /dev/test-vg/v5.1

# Add a thin snapshot of a dev that is already in the archive
Packing a new stream to an archive can be greatly sped up if you've already packed a snapshot of the device before.

> blk-archive unpack -a test-archive /dev/test-vg/v5.1 --delta-stream c11471f971310751 --delta-device /dev/test-vg/v5.0

Here we've already packed a snapshot called 'v5.0'.  We need to provide the path to the previously packed device (/dev/test-vg/v5.0).  This prior device must _not_ have been changed since it was packed.  We also provide the id of the stream within the archive.

Once the new snapshot (v5.1) has been packed we can delete the v5.0 snapshot and use the v5.1 snapshot for the next incremental backup.


# List streams in an archive
You can get a list of the streams that are in an archive using the *list* sub command:

>blk-archive list

```
cb917ad6b2bd1d7a  863303680 Mar 30 22 12:43 linux-v5.0.tar
53bfa67a08ba4342  871659520 Mar 30 22 12:43 linux-v5.1.tar
67d4b14604f7015c  871229440 Mar 30 22 12:43 linux-v5.2.tar
c11471f971310751  913336320 Mar 30 22 12:44 linux-v5.3.tar
4869608e5c757fb5  938137600 Mar 30 22 12:44 linux-v5.4.tar
d506b039bc99cd48  947691520 Mar 30 22 12:44 linux-v5.5.tar
5c5577a3e22665cb  957614080 Mar 30 22 12:45 linux-v5.6.tar
5f254e7b9132f3f5  965775360 Mar 30 22 12:45 linux-v5.7.tar
b5dca148918a6489  983869440 Mar 30 22 12:45 linux-v5.8.tar
0317a7046c72933b 1011015680 Mar 30 22 12:46 linux-v5.9.tar
5166cc67bffd870e 1019238400 Mar 30 22 12:46 linux-v5.10.tar
0d381b658b8d780a 1064212480 Mar 30 22 12:47 linux-v5.11.tar
6ffe73350b4f042a 1070315520 Mar 30 22 12:47 linux-v5.12.tar
a138c1aef20fd1bd 1091778560 Mar 30 22 12:47 linux-v5.13.tar
```

Each line describes a stream (a packed file or device).  There are four fields on each line:
- The stream unique identifier.  You will need this to unpack the stream at a later date.
- The size of the original file/device.
- The time it was placed in the archive.
- The basename of file/device.  In this case I was just packing linux tar files again.

# Restore to a file
To restore a stream you need to use the *unpack* sub command, along with the --create switch.

>blk-archive unpack -a test-archive --stream c11471f971310751 --create new-file

You specify the stream using it's unique identifier.  The positional argument is the destination.  

# Restore to a block device
Restoring to a block device, or a pre-existing file for that matter, is just the same as above except you omit the --create switch.

>blk-archive unpack -a test-archive --stream c11471f971310751 /dev/test-vg/v5.0-restored

The destination must exist, and be the same size as the stream.

# Restore to a thin device
Invoking a restore to a thin device is identical to any other block device.  However, blk-archive will read the mappings of the thin device and attempt to maximise data sharing within the pool.
- provisioned areas of the thin device that are unprovisioned in the stream will be discarded to unprovisioned them.
- before writing to a provisioned region blk-archive will check that it doesn't already contain identical data, and omit the write if so.  This avoids breaking sharing.

FIXME: come up with some good examples.  This is a killer feature so make sure it's compelling.

# Deleting a stream from an archive
You cannot delete a stream from an archive.  If you have an archive that contains many streams that you no longer want, then you should create a new archive and migrate the stream you wish to keep across to them.

# Migrating streams to a new archive
FIXME: finish

Use daily/monthly rolling snaps as example
