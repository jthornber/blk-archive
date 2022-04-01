# Alpha
These are the work items that need to be done before the alpha/internal release.  File formats will not be set in stone, so people should experiment with the tools, but not start using them for real data.

## Documentation
- [ ] Man page
- [ ] Use cases document [[Use Cases]]
- [ ] Design document
- [x] TODO list (this file)
- [x] README

## Code
- [ ] --delta switch to pack for packing thin deltas.
- [ ] Roll over slab files if they get too large.
- [x] Change --dir switch in create to -a to match other tools
- [x] If archive isn't specified try getting from env var (DM_ARCHIVE_DIR)
- [ ] verify doesn't handle unmapped regions, use Thin and Thick chunkers from pack
- [ ] specify data slab cache size in config file
- [ ] verify at end of pack by default.  Slower tools is better than losing data.
- [ ] Make sure unpack works with block devices as destination
- [ ] Unpack --minimal-writes flag for restoring to thins (or turn on automatically when restoring to thin.)
- [ ] Fix hang if pack is interrupted.  To do with shutting down compressor threads cleanly.
- [ ] unpack should only create a new file to restore to if the --create switch is given.
- [ ] Add --create switch to pack, this must be present to create a new output file.

# Beta
These are the work items that need to be done before the beta/-rc release.  At this point the formats will be set in stone, and supported in perpetuity, so people can start using the tool.
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

# Release
- [ ] Improve documentation.
- [ ] Sign off from QE.
- [ ] Perf team.
- [ ] Stratis.
- [ ] RHEL documentation.
- [ ] Work out RHEL support limits.

