# Alpha
These are the work items that need to be done before the alpha/internal release.  File formats will not be set in stone, so people should experiment with the tools, but not start using them for real data.

## Documentation
- [ ] Man page
- [ ] Use cases document [[Use Cases]]
- [X] Design document [Design]
- [x] TODO list (this file)
- [x] README

## Code
- [x] --delta switch to pack for packing thin deltas.
- [x] Change --dir switch in create to -a to match other tools
- [x] If archive isn't specified try getting from env var (DM_ARCHIVE_DIR)
- [x] verify doesn't handle unmapped regions, use Thin and Thick chunkers from pack
- [x] specify data slab cache size in config file
- [x] Make sure unpack works with block devices as destination
- [x] unpack should check destination size matches
- [x] what is unpack writing for unmapped to a thick device? (zeroes now)
- [x] Unpack --minimal-writes flag for restoring to thins (or turn on automatically when restoring to thin.)
- [ ] Fix hang if pack is interrupted.  To do with shutting down compressor threads cleanly.
- [x] unpack should only create a new file to restore to if the --create switch is given.
- [ ] create shouldn't use the env var
- [x] don't read hashes on start up
- [x] fast cdc
- [x] try zstd
- [ ] If discards are disabled we need to write zeroes
- [x] badly aligned discards should never happen, investigate

# Beta
These are the work items that need to be done before the beta/-rc release.  At this point the formats will be set in stone, and supported in perpetuity, so people can start using the tool.
- [ ] Are we coping with discarded deltas
- [ ] make slab size related to block size, eg, 1024 x block size, or make configurable?
- [ ] Roll over slab files if they get too large.
- [ ] Encryption
- [ ] Write front-end devel command that just does the split, dedup portion.  For benchmarking.
- [ ] Optimise the splitter.  Big perf improvement to be had here.
- [ ] dedup metadata streams.
- [ ] Improve efficiency of VMState.  Stack handling involves a lot of shifting up and down in arrays.
- [ ] Change VMState so top of stack is index 0, rather than 15.  Cosmetic.
- [ ] Multi thread unpack and verify.  unzipping slabs is the current bottleneck.
- [ ] Cope with damaged archive.  Test with damage of different sizes in different files.  This is a big piece of work.  I don't want to finalise the file formats until this is done since we'll have to add metadata to slab files to aid recovery.  Identify which streams are effected by any damage.
- [ ] Add fields to slab file header to describe it's contents and format version.
- [ ] Remote repositories.  Alpha release feedback needed to tell us how urgent this is.  We could postpone to a later release if not urgent.  Design should be done at this point though.
- [ ] *migrate* sub command to move streams between archives (essential for garbage collection since we can't delete streams)
- [ ] provide way to rebuild offsets file for slab files.  Compare timestamps and trigger automatically.
- [ ] add some way of tracking the block sizes output by the tracker
- [ ] endian testing.  Are the 256bit hashes endian specific?
- [ ] FileUnpackDest that uses fallocate for unmapped/zeroed areas?
- [ ] strace to see what io sizes actually are (lots of 8 byte reads when reading offsets/)

# Release
- [ ] Improve documentation.
- [ ] Sign off from QE.
- [ ] Perf team.
- [ ] Stratis.
- [ ] RHEL documentation.
- [ ] Work out RHEL support limits.

