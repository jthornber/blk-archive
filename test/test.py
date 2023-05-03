#!/usr/bin/python3
import datetime
import hashlib
import json
import os
import random
import shutil
import string
import subprocess
import sys
import tempfile

import unittest
import uuid
from enum import Enum
from pathlib import Path

# ALl the external binaries we use ...
LB_BIN = os.getenv('BLK_ARCHIVE_UT_LOSETUP', "/usr/sbin/losetup")
UMOUNT_BIN = os.getenv('BLK_ARCHIVE_UT_UMOUNT', "/usr/bin/umount")
FS_MAKE_BIN = os.getenv("BLK_ARCHIVE_UT_FS_MAKE", "/usr/sbin/mkfs.xfs")
MOUNT_BIN = os.getenv("BLK_ARCHIVE_UT_MOUNT", "/usr/bin/mount")
LVM_BIN = os.getenv("BLK_ARCHIVE_UT_LVM", "/usr/sbin/lvm")
CMP_BIN = os.getenv("BLK_ARCHIVE_UT_CMP", "/usr/bin/cmp")
BLK_ARCHIVE_BIN = os.getenv("BLK_ARCHIVE_UT_BIN",
                            os.path.join(os.path.dirname(Path(__file__).parent), "target", "release", "blk-archive"))

FS_TYPE = "xfs"     # Has check-summing which detects data corruption with fs well
SEED = int(os.getenv("BLK_ARCHIVE_UT_SEED", 0))

POOL_SIZE_MB = 8000
BASIC_BLOCK_SIZE_MB = 400

PERSISTENT = None

cs = list(string.ascii_uppercase + string.ascii_lowercase + string.digits)


def rs(str_len):
    return ''.join(random.choice(cs) for _ in range(str_len))


BLOCK_SIZE = 512
MAX_FILE_SIZE = 1024*1024*8
DUPLICATE_DATA = rs(MAX_FILE_SIZE)


def _round_to_block_size(size):
    return size if size % BLOCK_SIZE == 0 else size + BLOCK_SIZE - size % BLOCK_SIZE


def disk_usage(path):
    st = os.statvfs(path)
    free = st.f_bavail * st.f_frsize
    total = st.f_blocks * st.f_frsize
    return total, free


def md5(t):
    h = hashlib.md5(usedforsecurity=False)
    h.update(t.encode("utf-8"))
    return h.hexdigest()


class LoopBackDevices(object):

    def __init__(self, test_dir):
        self.test_dir = test_dir
        self.count = 0
        self.devices = {}

    def create_device(self, size_mib):
        """
        Create a new loop back device.
        :param size_mib:
        :return: opaque handle
        """
        backing_file = os.path.join(self.test_dir, f"block_device_{self.count}")
        self.count += 1

        with open(backing_file, 'ab') as bd:
            bd.truncate(size_mib * (1024 * 1024))

        result = subprocess.run([LB_BIN, '-f', '--show', backing_file],
                                check=True, stdout=subprocess.PIPE)
        device = str.strip(result.stdout.decode("utf-8"))
        token = uuid.uuid4()
        self.devices[token] = (device, backing_file)
        return token

    def device_node(self, token):
        if token in self.devices:
            return self.devices[token][0]

    def destroy_all(self):
        # detach the devices and delete the file(s) and directory!
        for (device, backing_file) in self.devices.values():
            subprocess.run([LB_BIN, '-d', device], check=True)
            os.remove(backing_file)

        self.devices = {}
        self.count = 0
        self.test_dir = None


class Data:

    class Type(Enum):
        BASIC = 1
        DM_THICK = 2
        DM_THIN = 3
        FILE = 4
        UNKNOWN = 5

    def __str__(self):
        return "%s" % self.t

    def __init__(self, data_type, pd):
        self.t = data_type
        self.filled = False
        self.mount_path = None
        self.device_node = None
        self.pd = pd
        self.fs_created = False
        if data_type == Data.Type.FILE:
            self.mount_path = os.path.join(pd.test_dir, f"file_{rs(5)}")
        else:
            if data_type == Data.Type.BASIC:
                token = self.pd.lb.create_device(BASIC_BLOCK_SIZE_MB)
                self.device_node = self.pd.lb.device_node(token)
            elif data_type == Data.Type.DM_THIN:
                self.device_node = self.pd.make_dm_thin()
            elif data_type == Data.Type.DM_THICK:
                self.device_node = self.pd.make_dm_thick()

    def mount(self):
        if self.t == Data.Type.FILE or self.mount_path is not None:
            return self

        self.mount_path = os.path.join(os.sep, "mnt", str(uuid.uuid4()))
        os.makedirs(self.mount_path)
        subprocess.run([MOUNT_BIN, self.device_node, self.mount_path], check=True)
        return self

    def unmount(self):
        if self.t == Data.Type.FILE or self.mount_path is None:
            return self
        subprocess.run([UMOUNT_BIN, self.mount_path], check=True)
        os.rmdir(self.mount_path)
        self.mount_path = None
        return self

    def create_fs(self):
        if self.t == Data.Type.FILE or self.fs_created:
            return self

        subprocess.run([FS_MAKE_BIN, self.device_node], check=True, capture_output=True)
        self.fs_created = True
        return self

    def destroy(self):
        self.unmount()
        if self.t == Data.Type.FILE:
            if os.path.isfile(self.mount_path):
                os.remove(self.mount_path)

        self.pd = None
        self.mount_path = None
        self.device_node = None
        self.t = Data.Type.UNKNOWN

    def compare(self, rvalue):
        # We are either comparing a blk device to a blk device or a file to a blkdevice or vis versa
        l_f = self.mount_path if self.mount_path is not None else self.device_node
        r_f = rvalue.mount_path if rvalue.mount_path is not None else rvalue.device_node

        # filecmp.cmp does not work..., cmp can check 400MB in 0.3-0.4 seconds
        return subprocess.run([CMP_BIN, l_f, r_f], check=False).returncode == 0

    @staticmethod
    def _fill_file(file):
        size = _round_to_block_size(random.randint(BLOCK_SIZE, MAX_FILE_SIZE))
        with open(file, 'w') as out:
            out.write(DUPLICATE_DATA[0:size])
            out.flush()
            os.fsync(out.fileno())

    def fill(self):
        if not self.filled:
            if self.mount_path is not None:
                if self.t == Data.Type.FILE:
                    Data._fill_file(self.mount_path)
                else:
                    # TODO: Create 1 file on the mount point for now, will expand later.
                    fn = os.path.join(os.sep, self.mount_path, rs(10))
                    Data._fill_file(fn)
            else:
                # TODO: Write directly to block device or consider this an error?
                pass

            self.filled = True
        return self

    def fs_path(self):
        return self.mount_path

    def dev_node(self):
        return self.device_node

    def src_arg(self):
        if self.t == Data.Type.FILE:
            return [self.mount_path]
        return [self.device_node]

    def dest_arg(self):
        if self.t == Data.Type.FILE:
            return ["--create", self.mount_path]
        return [self.device_node]


class UnitTestData:

    def __init__(self, test_dir):
        self.mounted = []
        self.test_dir = test_dir
        self.lb = LoopBackDevices(test_dir)

        # Keep track of things
        self.to_clean_up = []
        self.data = []

    def _destroy(self):
        # Data objects we have handed out get cleaned up first
        for d in self.data:
            d.destroy()

        # Then run clean-up commands in reverse
        self.to_clean_up.reverse()
        for i in self.to_clean_up:
            subprocess.run(i, check=True, capture_output=True)
        self.to_clean_up = []

        # Remove all loop back devices
        self.lb.destroy_all()

    def _make_dm_resource(self, block_dev, vg_name="blkarchive_test"):
        # Create VG
        subprocess.run([LVM_BIN, "vgcreate", vg_name, block_dev],
                       check=True, capture_output=True)
        self.to_clean_up.append([LVM_BIN, "vgremove", "-f", vg_name])

        # Create thin pool
        subprocess.run([LVM_BIN, "lvcreate", "-L", f"{BASIC_BLOCK_SIZE_MB * 3}M", "-T", f"{vg_name}/thinpool"],
                       check=True, capture_output=True)

        return (vg_name, f"{vg_name}/thinpool")

    def make_dm_thin(self):
        name = f"thin_lv_{rs(8)}"
        subprocess.run([LVM_BIN, "lvcreate", "-V", f"{BASIC_BLOCK_SIZE_MB}M", "-T", f"{self.vg}/thinpool", "-n", name],
                       check=True, capture_output=True)
        return os.path.join(os.sep, "dev", self.vg, name)

    def make_dm_thick(self):
        name = f"thick_lv_{rs(8)}"
        subprocess.run([LVM_BIN, "lvcreate", "-L", f"{BASIC_BLOCK_SIZE_MB}M", self.vg, "-n", name],
                       check=True, capture_output=True)
        return os.path.join(os.sep, "dev", self.vg, name)

    def setup(self):
        pool_token = self.lb.create_device(POOL_SIZE_MB)
        (self.vg, self.thin_pool) = self._make_dm_resource(self.lb.device_node(pool_token), f"blkarchive_test_{rs(8)}")

    def teardown(self):
        self._destroy()

    def create(self, d_types, count=1):
        rc = []
        for _ in range(count):
            for d in d_types:
                d = Data(d, self)
                self.data.append(d)
                rc.append(d)
        return rc


class BlkArchive(unittest.TestCase):

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(prefix='blk_archive_unit_test_', dir="/")
        self.p = UnitTestData(self.test_dir)
        self.p.setup()

    def tearDown(self):
        self.p.teardown()
        # Could change this to shutil.rmtree, but this should work if everything is cleaning up correctly
        os.rmdir(self.test_dir)

    @staticmethod
    def _cmd(cmd, add_arg=None, output=False, parse_json=False):
        if add_arg is not None:
            cmd.extend(add_arg)

        capture = True if output or parse_json else False
        result = subprocess.run(cmd, check=True, capture_output=capture)
        if parse_json:
            try:
                return json.loads(result.stdout)
            except json.decoder.JSONDecodeError as e:
                print(f"Got a JSON decoder error with\n>>>\n{result.stdout}\n<<<")
                raise e

    def test_source_block_combinations(self):
        src = self.p.create([Data.Type.BASIC, Data.Type.DM_THIN, Data.Type.DM_THICK])

        archive = os.path.join(self.test_dir, f"test_archive_{rs(8)}")
        try:
            # create archive
            BlkArchive._cmd([BLK_ARCHIVE_BIN, "-j", "create", "-a", archive])

            for s in src:
                s.create_fs().mount().fill().unmount()

                # Pack source
                result = BlkArchive._cmd([BLK_ARCHIVE_BIN, "-j", "pack", "-a", archive], s.src_arg(),
                                         parse_json=True)
                stream_id = result["stream_id"]

                # For now, ensure dump-stream doesn't panic
                BlkArchive._cmd([BLK_ARCHIVE_BIN, "dump-stream", "-a", archive, "-s", stream_id],
                                output=True)

                # Unpack source to each of the different destination targets
                for d in self.p.create([Data.Type.BASIC, Data.Type.FILE, Data.Type.DM_THIN, Data.Type.DM_THICK]):
                    BlkArchive._cmd([BLK_ARCHIVE_BIN, "-j", "unpack", "-a", archive,
                                     "-s", stream_id], d.dest_arg())

                    BlkArchive._cmd([BLK_ARCHIVE_BIN, "-j", "verify", "-a", archive,
                                     "-s", stream_id], s.src_arg())

                    # MAYBE blk-archive -j verify -a <archive> -s <stream> d.dest_arg()
                    self.assertTrue(s.compare(d), f"Data miss-compare for {s} == {d}")

                    d.unmount()
                    d.destroy()
        finally:
            shutil.rmtree(archive)

    def test_files(self):
        def check_listing(streams):
            stream_list = BlkArchive._cmd([BLK_ARCHIVE_BIN, "-j", "list", "-a", archive], parse_json=True)
            found = 0
            for stream_id_to_check in streams:
                for i in stream_list:
                    if i["stream_id"] == stream_id_to_check:
                        found += 1
            return True if len(streams) == found else False

        stream_ids = []

        # Create an archive of files, testing listing.
        src = self.p.create([Data.Type.FILE], random.randrange(1, 20))
        archive = os.path.join(self.test_dir, f"test_archive_{rs(8)}")
        BlkArchive._cmd([BLK_ARCHIVE_BIN, "-j", "create", "-a", archive])
        try:
            for s in src:
                s.fill()
                result = BlkArchive._cmd([BLK_ARCHIVE_BIN, "-j", "pack", "-a", archive], s.src_arg(),
                                         parse_json=True)
                stream_ids.append(result["stream_id"])
                self.assertTrue(check_listing(stream_ids), "Missing or extra streams in archive listing...")
        finally:
            shutil.rmtree(archive)

    def test_empty_thin(self):
        # Test thin block device source that is simply empty
        archive = os.path.join(self.test_dir, f"test_archive_{rs(8)}")
        # create archive
        BlkArchive._cmd([BLK_ARCHIVE_BIN, "-j", "create", "-a", archive])
        try:
            (s, d) = self.p.create([Data.Type.DM_THIN, Data.Type.FILE])
            result = BlkArchive._cmd([BLK_ARCHIVE_BIN, "-j", "pack", "-a", archive], s.src_arg(),
                                     parse_json=True)
            stream_id = result["stream_id"]
            BlkArchive._cmd([BLK_ARCHIVE_BIN, "-j", "unpack", "-a", archive,
                             "-s", stream_id], d.dest_arg())
            self.assertTrue(s.compare(d), f"Data miss-compare for {s} == {d}")
            s.destroy()
            d.destroy()
        finally:
            shutil.rmtree(archive)


    def test_rust_unit_tests(self):
        subprocess.run(["cargo", "test"], check=True)


if __name__ == '__main__':
    SEED = SEED if SEED != 0 else int(datetime.datetime.now().microsecond)
    random.seed(SEED)
    print(f'Use BLK_ARCHIVE_UT_SEED={SEED} to re-create test run')
    if os.path.exists(BLK_ARCHIVE_BIN):
        print(f"Using: {BLK_ARCHIVE_BIN}")
    else:
        print(f"Unable to find blk-archive bin @ {BLK_ARCHIVE_BIN},"
              f" please build before running tests or export BLK_ARCHIVE_UT_BIN='<test binary>' to the binary to test")
        sys.exit(1)
    unittest.main()
