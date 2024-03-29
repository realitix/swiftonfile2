# Copyright (c) 2012-2013 Red Hat, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" Tests for common.utils """

import os
import unittest
import errno
import xattr
import pickle
import tempfile
import hashlib
import shutil
import mock
import uuid
from collections import defaultdict
from mock import patch, Mock
from swiftonfile.swift.common import utils
from swiftonfile.swift.common.utils import (
    deserialize_metadata,
    serialize_metadata,
    PICKLE_PROTOCOL,
)
from swiftonfile.swift.common.exceptions import (
    SwiftOnFileSystemOSError,
    SwiftOnFileSystemIOError,
)
from swift.common.exceptions import DiskFileNoSpace

#
# Somewhat hacky way of emulating the operation of xattr calls. They are made
# against a dictionary that stores the xattr key/value pairs.
#
_xattrs = {}
_xattr_op_cnt = defaultdict(int)
_xattr_set_err = {}
_xattr_get_err = {}
_xattr_rem_err = {}
_xattr_set = None
_xattr_get = None
_xattr_remove = None


def _xkey(path, key):
    return "%s:%s" % (path, key)


def _setxattr(path, key, value, *args, **kwargs):
    _xattr_op_cnt["set"] += 1
    xkey = _xkey(path, key)
    if xkey in _xattr_set_err:
        e = IOError()
        e.errno = _xattr_set_err[xkey]
        raise e
    global _xattrs
    _xattrs[xkey] = value


def _getxattr(path, key, *args, **kwargs):
    _xattr_op_cnt["get"] += 1
    xkey = _xkey(path, key)
    if xkey in _xattr_get_err:
        e = IOError()
        e.errno = _xattr_get_err[xkey]
        raise e
    global _xattrs
    if xkey in _xattrs:
        ret_val = _xattrs[xkey]
    else:
        e = IOError("Fake IOError")
        e.errno = errno.ENODATA
        raise e
    return ret_val


def _removexattr(path, key, *args, **kwargs):
    _xattr_op_cnt["remove"] += 1
    xkey = _xkey(path, key)
    if xkey in _xattr_rem_err:
        e = IOError()
        e.errno = _xattr_rem_err[xkey]
        raise e
    global _xattrs
    if xkey in _xattrs:
        del _xattrs[xkey]
    else:
        e = IOError("Fake IOError")
        e.errno = errno.ENODATA
        raise e


def _initxattr():
    global _xattrs
    _xattrs = {}
    global _xattr_op_cnt
    _xattr_op_cnt = defaultdict(int)
    global _xattr_set_err, _xattr_get_err, _xattr_rem_err
    _xattr_set_err = {}
    _xattr_get_err = {}
    _xattr_rem_err = {}

    # Save the current methods
    global _xattr_set
    _xattr_set = xattr.setxattr
    global _xattr_get
    _xattr_get = xattr.getxattr
    global _xattr_remove
    _xattr_remove = xattr.removexattr

    # Monkey patch the calls we use with our internal unit test versions
    xattr.setxattr = _setxattr
    xattr.getxattr = _getxattr
    xattr.removexattr = _removexattr


def _destroyxattr():
    # Restore the current methods just in case
    global _xattr_set
    xattr.setxattr = _xattr_set
    global _xattr_get
    xattr.getxattr = _xattr_get
    global _xattr_remove
    xattr.removexattr = _xattr_remove
    # Destroy the stored values and
    global _xattrs
    _xattrs = None


class SimMemcache:
    def __init__(self):
        self._d = {}

    def get(self, key):
        return self._d.get(key, None)

    def set(self, key, value):
        self._d[key] = value


def _mock_os_fsync(fd):
    return


class TestSafeUnpickler(unittest.TestCase):
    class Exploit:
        def __reduce__(self):
            return (os.system, ("touch /tmp/pickle-exploit",))

    def test_loads(self):
        valid_md = {"key1": "val1", "key2": "val2"}
        for protocol in (0, 1, 2):
            valid_dump = pickle.dumps(valid_md, protocol)
            mal_dump = pickle.dumps(self.Exploit(), protocol)
            # malicious dump is appended to valid dump
            payload1 = valid_dump[:-1] + mal_dump
            # malicious dump is prefixed to valid dump
            payload2 = mal_dump[:-1] + valid_dump
            # entire dump is malicious
            payload3 = mal_dump
            for payload in (payload1, payload2, payload3):
                try:
                    utils.SafeUnpickler.loads(payload)
                except pickle.UnpicklingError as err:
                    self.assertTrue("Potentially unsafe pickle" in str(err))
                else:
                    self.fail("Expecting pickle.UnpicklingError")


class TestUtils(unittest.TestCase):
    """Tests for common.utils"""

    def setUp(self):
        _initxattr()

    def tearDown(self):
        _destroyxattr()

    def test_write_metadata(self):
        path = "/tmp/foo/w"
        orig_d = {"bar": "foo"}
        utils.write_metadata(path, orig_d)
        xkey = _xkey(path, utils.METADATA_KEY)
        assert len(_xattrs.keys()) == 1
        assert xkey in _xattrs
        assert orig_d == deserialize_metadata(_xattrs[xkey])
        assert _xattr_op_cnt["set"] == 1

    def test_write_metadata_err(self):
        path = "/tmp/foo/w"
        orig_d = {"bar": "foo"}
        xkey = _xkey(path, utils.METADATA_KEY)
        _xattr_set_err[xkey] = errno.EOPNOTSUPP
        try:
            utils.write_metadata(path, orig_d)
        except IOError as e:
            assert e.errno == errno.EOPNOTSUPP
            assert len(_xattrs.keys()) == 0
            assert _xattr_op_cnt["set"] == 1
        else:
            self.fail("Expected an IOError exception on write")

    def test_write_metadata_space_err(self):
        def _mock_xattr_setattr(item, name, value):
            raise IOError(errno.ENOSPC, os.strerror(errno.ENOSPC))

        with patch("xattr.setxattr", _mock_xattr_setattr):
            path = "/tmp/foo/w"
            orig_d = {"bar": "foo"}
            try:
                utils.write_metadata(path, orig_d)
            except DiskFileNoSpace:
                pass
            else:
                self.fail("Expected DiskFileNoSpace exception")
            fd = 0
            try:
                utils.write_metadata(fd, orig_d)
            except DiskFileNoSpace:
                pass
            else:
                self.fail("Expected DiskFileNoSpace exception")

    def test_write_metadata_multiple(self):
        # At 64 KB an xattr key/value pair, this should generate three keys.
        path = "/tmp/foo/w"
        orig_d = {"bar": "x" * 150000}
        utils.write_metadata(path, orig_d)
        assert len(_xattrs.keys()) == 3, "Expected 3 keys, found %d" % len(
            _xattrs.keys()
        )
        payload = b""
        for i in range(0, 3):
            xkey = _xkey(path, "%s%s" % (utils.METADATA_KEY, i or ""))
            assert xkey in _xattrs
            assert len(_xattrs[xkey]) <= utils.MAX_XATTR_SIZE
            payload += _xattrs[xkey]
        assert orig_d == deserialize_metadata(payload)
        assert _xattr_op_cnt["set"] == 3, "%r" % _xattr_op_cnt

    def test_clean_metadata(self):
        path = "/tmp/foo/c"
        expected_d = {"a": "y" * 150000}
        expected_p = serialize_metadata(expected_d)
        for i in range(0, 3):
            xkey = _xkey(path, "%s%s" % (utils.METADATA_KEY, i or ""))
            _xattrs[xkey] = expected_p[: utils.MAX_XATTR_SIZE]
            expected_p = expected_p[utils.MAX_XATTR_SIZE :]
        assert not expected_p
        utils.clean_metadata(path)
        assert _xattr_op_cnt["remove"] == 4, "%r" % _xattr_op_cnt

    def test_clean_metadata_err(self):
        path = "/tmp/foo/c"
        xkey = _xkey(path, utils.METADATA_KEY)
        _xattrs[xkey] = serialize_metadata({"a": "y"})
        _xattr_rem_err[xkey] = errno.EOPNOTSUPP
        try:
            utils.clean_metadata(path)
        except IOError as e:
            assert e.errno == errno.EOPNOTSUPP
            assert _xattr_op_cnt["remove"] == 1, "%r" % _xattr_op_cnt
        else:
            self.fail("Expected an IOError exception on remove")

    def test_read_metadata(self):
        path = "/tmp/foo/r"
        expected_d = {"a": "y"}
        xkey = _xkey(path, utils.METADATA_KEY)
        _xattrs[xkey] = serialize_metadata(expected_d)
        res_d = utils.read_metadata(path)
        assert res_d == expected_d, "Expected %r, result %r" % (expected_d, res_d)
        assert _xattr_op_cnt["get"] == 1, "%r" % _xattr_op_cnt

    def test_read_metadata_notfound(self):
        path = "/tmp/foo/r"
        res_d = utils.read_metadata(path)
        assert res_d == {}
        assert _xattr_op_cnt["get"] == 1, "%r" % _xattr_op_cnt

    def test_read_metadata_err(self):
        path = "/tmp/foo/r"
        expected_d = {"a": "y"}
        xkey = _xkey(path, utils.METADATA_KEY)
        _xattrs[xkey] = serialize_metadata(expected_d)
        _xattr_get_err[xkey] = errno.EOPNOTSUPP
        try:
            utils.read_metadata(path)
        except IOError as e:
            assert e.errno == errno.EOPNOTSUPP
            assert _xattr_op_cnt["get"] == 1, "%r" % _xattr_op_cnt
        else:
            self.fail("Expected an IOError exception on get")

    def test_read_metadata_multiple(self):
        path = "/tmp/foo/r"
        expected_d = {"a": "y" * 150000}
        expected_p = serialize_metadata(expected_d)
        for i in range(0, 3):
            xkey = _xkey(path, "%s%s" % (utils.METADATA_KEY, i or ""))
            _xattrs[xkey] = expected_p[: utils.MAX_XATTR_SIZE]
            expected_p = expected_p[utils.MAX_XATTR_SIZE :]
        assert not expected_p
        res_d = utils.read_metadata(path)
        assert res_d == expected_d, "Expected %r, result %r" % (expected_d, res_d)
        assert _xattr_op_cnt["get"] == 4, "%r" % _xattr_op_cnt

    def test_read_metadata_multiple_one_missing(self):
        path = "/tmp/foo/r"
        expected_d = {"a": "y" * 150000}
        expected_p = serialize_metadata(expected_d)
        for i in range(0, 2):
            xkey = _xkey(path, "%s%s" % (utils.METADATA_KEY, i or ""))
            _xattrs[xkey] = expected_p[: utils.MAX_XATTR_SIZE]
            expected_p = expected_p[utils.MAX_XATTR_SIZE :]
        assert len(expected_p) <= utils.MAX_XATTR_SIZE
        res_d = utils.read_metadata(path)
        assert res_d == {}
        assert _xattr_op_cnt["get"] == 3, "%r" % _xattr_op_cnt

    def test_restore_metadata_none(self):
        # No initial metadata
        path = "/tmp/foo/i"
        res_d = utils.restore_metadata(path, {"b": "y"}, {})
        expected_d = {"b": "y"}
        assert res_d == expected_d, "Expected %r, result %r" % (expected_d, res_d)
        assert _xattr_op_cnt["set"] == 1, "%r" % _xattr_op_cnt

    def test_restore_metadata(self):
        # Initial metadata
        path = "/tmp/foo/i"
        initial_d = {"a": "z"}
        xkey = _xkey(path, utils.METADATA_KEY)
        _xattrs[xkey] = serialize_metadata(initial_d)
        res_d = utils.restore_metadata(path, {"b": "y"}, initial_d)
        expected_d = {"a": "z", "b": "y"}
        assert res_d == expected_d, "Expected %r, result %r" % (expected_d, res_d)
        assert _xattr_op_cnt["set"] == 1, "%r" % _xattr_op_cnt

    def test_restore_metadata_nochange(self):
        # Initial metadata but no changes
        path = "/tmp/foo/i"
        initial_d = {"a": "z"}
        xkey = _xkey(path, utils.METADATA_KEY)
        _xattrs[xkey] = serialize_metadata(initial_d)
        res_d = utils.restore_metadata(path, {}, initial_d)
        expected_d = {"a": "z"}
        assert res_d == expected_d, "Expected %r, result %r" % (expected_d, res_d)
        assert _xattr_op_cnt["set"] == 0, "%r" % _xattr_op_cnt

    def test_deserialize_metadata_pickle(self):
        orig_md = {"key1": "value1", "key2": "value2"}
        pickled_md = pickle.dumps(orig_md, PICKLE_PROTOCOL)
        _m_pickle_loads = Mock(return_value={})
        utils.read_pickled_metadata = True
        with patch("swiftonfile.swift.common.utils.pickle.loads", _m_pickle_loads):
            # pickled
            md = utils.deserialize_metadata(pickled_md)
            self.assertTrue(_m_pickle_loads.called)
            self.assertTrue(isinstance(md, dict))
            _m_pickle_loads.reset_mock()
            # not pickled
            utils.deserialize_metadata("not_pickle")
            self.assertFalse(_m_pickle_loads.called)
            _m_pickle_loads.reset_mock()
            # pickled but conf does not allow loading
            utils.read_pickled_metadata = False
            md = utils.deserialize_metadata(pickled_md)
            self.assertFalse(_m_pickle_loads.called)

            # malformed pickle
            _m_pickle_loads.side_effect = pickle.UnpicklingError
            md = utils.deserialize_metadata("malformed_pickle")
            self.assertTrue(isinstance(md, dict))

    def test_deserialize_metadata_json(self):
        _m_json_loads = Mock(return_value={})
        with patch("swiftonfile.swift.common.utils.json.loads", _m_json_loads):
            utils.deserialize_metadata("not_json")
            self.assertFalse(_m_json_loads.called)
            _m_json_loads.reset_mock()
            utils.deserialize_metadata("{fake_valid_json}")
            self.assertTrue(_m_json_loads.called)

    def test_get_etag_empty(self):
        tf = tempfile.NamedTemporaryFile()
        hd = utils._get_etag(tf.name)
        assert hd == hashlib.md5().hexdigest()

    def test_get_etag(self):
        tf = tempfile.NamedTemporaryFile()
        tf.file.write(b"123" * utils.CHUNK_SIZE)
        tf.file.flush()
        hd = utils._get_etag(tf.name)
        tf.file.seek(0)
        md5 = hashlib.md5()
        while True:
            chunk = tf.file.read(utils.CHUNK_SIZE)
            if not chunk:
                break
            md5.update(chunk)
        assert hd == md5.hexdigest()

    def test__read_for_etag(self):
        with mock.patch.object(utils, "do_read", return_value=False):
            utils._read_for_etag(1)

    def test_get_etag_dup_fd_closed(self):
        fd, path = tempfile.mkstemp()
        data = b"It's not who we are underneath, but what we do that defines us"
        os.write(fd, data)
        os.lseek(fd, 0, os.SEEK_SET)

        mock_do_close = Mock()
        with patch("swiftonfile.swift.common.utils.do_close", mock_do_close):
            etag = utils._get_etag(fd)
        self.assertEqual(etag, hashlib.md5(data).hexdigest())
        self.assertTrue(mock_do_close.called)

        # We mocked out close, so we have to close the fd for real
        os.close(mock_do_close.call_args[0][0])
        os.close(fd)

    def test_get_object_metadata_dne(self):
        md = utils.get_object_metadata("/tmp/doesNotEx1st")
        assert md == {}

    def test_get_object_metadata_err(self):
        tf = tempfile.NamedTemporaryFile()
        try:
            utils.get_object_metadata(os.path.join(tf.name, "doesNotEx1st"))
        except SwiftOnFileSystemOSError as e:
            assert e.errno != errno.ENOENT
        else:
            self.fail("Expected exception")

    obj_keys = (
        utils.X_TIMESTAMP,
        utils.X_CONTENT_TYPE,
        utils.X_ETAG,
        utils.X_CONTENT_LENGTH,
        utils.X_TYPE,
        utils.X_OBJECT_TYPE,
    )

    def test_get_object_metadata_file(self):
        tf = tempfile.NamedTemporaryFile()
        tf.file.write(b"123")
        tf.file.flush()
        md = utils.get_object_metadata(tf.name)
        for key in self.obj_keys:
            assert key in md, "Expected key %s in %r" % (key, md)
        assert md[utils.X_TYPE] == utils.OBJECT
        assert md[utils.X_OBJECT_TYPE] == utils.FILE
        assert md[utils.X_CONTENT_TYPE] == utils.FILE_TYPE
        assert md[utils.X_CONTENT_LENGTH] == os.path.getsize(tf.name)
        assert md[utils.X_TIMESTAMP] == utils.normalize_timestamp(
            os.path.getctime(tf.name)
        )
        assert md[utils.X_ETAG] == utils._get_etag(tf.name)

    def test_get_object_metadata_file_int(self):
        with mock.patch.object(utils, "do_fstat", return_value=None) as do_fstat:
            utils.get_object_metadata(1)
            do_fstat.assert_called()

    def test_get_object_metadata_dir(self):
        td = tempfile.mkdtemp()
        try:
            md = utils.get_object_metadata(td)
            for key in self.obj_keys:
                assert key in md, "Expected key %s in %r" % (key, md)
            assert md[utils.X_TYPE] == utils.OBJECT
            assert md[utils.X_OBJECT_TYPE] == utils.DIR_NON_OBJECT
            assert md[utils.X_CONTENT_TYPE] == utils.DIR_TYPE
            assert md[utils.X_CONTENT_LENGTH] == 0
            assert md[utils.X_TIMESTAMP] == utils.normalize_timestamp(
                os.path.getctime(td)
            )
            assert md[utils.X_ETAG] == hashlib.md5().hexdigest()
        finally:
            os.rmdir(td)

    def test_create_object_metadata_file(self):
        tf = tempfile.NamedTemporaryFile()
        tf.file.write(b"4567")
        tf.file.flush()
        r_md = utils.create_object_metadata(tf.name)

        xkey = _xkey(tf.name, utils.METADATA_KEY)
        assert len(_xattrs.keys()) == 1
        assert xkey in _xattrs
        assert _xattr_op_cnt["set"] == 1
        md = deserialize_metadata(_xattrs[xkey])
        assert r_md == md

        for key in self.obj_keys:
            assert key in md, "Expected key %s in %r" % (key, md)
        assert md[utils.X_TYPE] == utils.OBJECT
        assert md[utils.X_OBJECT_TYPE] == utils.FILE
        assert md[utils.X_CONTENT_TYPE] == utils.FILE_TYPE
        assert md[utils.X_CONTENT_LENGTH] == os.path.getsize(tf.name)
        assert md[utils.X_TIMESTAMP] == utils.normalize_timestamp(
            os.path.getctime(tf.name)
        )
        assert md[utils.X_ETAG] == utils._get_etag(tf.name)

    def test_create_object_metadata_dir(self):
        td = tempfile.mkdtemp()
        try:
            r_md = utils.create_object_metadata(td)

            xkey = _xkey(td, utils.METADATA_KEY)
            assert len(_xattrs.keys()) == 1
            assert xkey in _xattrs
            assert _xattr_op_cnt["set"] == 1
            md = deserialize_metadata(_xattrs[xkey])
            assert r_md == md

            for key in self.obj_keys:
                assert key in md, "Expected key %s in %r" % (key, md)
            assert md[utils.X_TYPE] == utils.OBJECT
            assert md[utils.X_OBJECT_TYPE] == utils.DIR_NON_OBJECT
            assert md[utils.X_CONTENT_TYPE] == utils.DIR_TYPE
            assert md[utils.X_CONTENT_LENGTH] == 0
            assert md[utils.X_TIMESTAMP] == utils.normalize_timestamp(
                os.path.getctime(td)
            )
            assert md[utils.X_ETAG] == hashlib.md5().hexdigest()
        finally:
            os.rmdir(td)

    def test_validate_object_empty(self):
        ret = utils.validate_object({})
        assert not ret

    def test_validate_object_missing_keys(self):
        ret = utils.validate_object({"foo": "bar"})
        assert not ret

    def test_validate_object_bad_type(self):
        md = {
            utils.X_TIMESTAMP: "na",
            utils.X_CONTENT_TYPE: "na",
            utils.X_ETAG: "bad",
            utils.X_CONTENT_LENGTH: "na",
            utils.X_TYPE: "bad",
            utils.X_OBJECT_TYPE: "na",
        }
        ret = utils.validate_object(md)
        assert not ret

    def test_validate_object_good_type(self):
        md = {
            utils.X_TIMESTAMP: "na",
            utils.X_CONTENT_TYPE: "na",
            utils.X_ETAG: "bad",
            utils.X_CONTENT_LENGTH: "na",
            utils.X_TYPE: utils.OBJECT,
            utils.X_OBJECT_TYPE: "na",
        }
        ret = utils.validate_object(md)
        assert ret

    def test_validate_object_with_stat(self):
        md = {
            utils.X_TIMESTAMP: "na",
            utils.X_CONTENT_TYPE: "na",
            utils.X_ETAG: "bad",
            utils.X_CONTENT_LENGTH: "12345",
            utils.X_TYPE: utils.OBJECT,
            utils.X_OBJECT_TYPE: "na",
        }
        fake_stat = Mock(st_size=12346, st_mode=33188)
        self.assertFalse(utils.validate_object(md, fake_stat))
        fake_stat = Mock(st_size=12345, st_mode=33188)
        self.assertTrue(utils.validate_object(md, fake_stat))

    def test_write_pickle(self):
        td = tempfile.mkdtemp()
        try:
            fpp = os.path.join(td, "pp")
            # FIXME: Remove this patch when coverage.py can handle eventlet
            with patch("swiftonfile.swift.common.fs_utils.do_fsync", _mock_os_fsync):
                utils.write_pickle("pickled peppers", fpp)
            with open(fpp, "rb") as f:
                contents = f.read()
            s = pickle.loads(contents)
            assert s == "pickled peppers", repr(s)
        finally:
            shutil.rmtree(td)

    def test_write_pickle_ignore_tmp(self):
        tf = tempfile.NamedTemporaryFile()
        td = tempfile.mkdtemp()
        try:
            fpp = os.path.join(td, "pp")
            # Also test an explicity pickle protocol
            # FIXME: Remove this patch when coverage.py can handle eventlet
            with patch("swiftonfile.swift.common.fs_utils.do_fsync", _mock_os_fsync):
                utils.write_pickle(
                    "pickled peppers", fpp, tmp=tf.name, pickle_protocol=2
                )
            with open(fpp, "rb") as f:
                contents = f.read()
            s = pickle.loads(contents)
            assert s == "pickled peppers", repr(s)
            with open(tf.name, "rb") as f:
                contents = f.read()
            assert contents == b""
        finally:
            shutil.rmtree(td)


class TestUtilsDirObjects(unittest.TestCase):
    def setUp(self):
        _initxattr()
        self.dirs = ["dir1", "dir1/dir2", "dir1/dir2/dir3"]
        self.files = ["file1", "file2", "dir1/dir2/file3"]
        self.tempdir = tempfile.mkdtemp()
        self.rootdir = os.path.join(self.tempdir, "a")
        for d in self.dirs:
            os.makedirs(os.path.join(self.rootdir, d))
        for f in self.files:
            open(os.path.join(self.rootdir, f), "w").close()

    def tearDown(self):
        _destroyxattr()
        shutil.rmtree(self.tempdir)

    def _set_dir_object(self, obj):
        metadata = utils.read_metadata(os.path.join(self.rootdir, obj))
        metadata[utils.X_OBJECT_TYPE] = utils.DIR_OBJECT
        utils.write_metadata(os.path.join(self.rootdir, self.dirs[0]), metadata)

    def _clear_dir_object(self, obj):
        metadata = utils.read_metadata(os.path.join(self.rootdir, obj))
        metadata[utils.X_OBJECT_TYPE] = utils.DIR_NON_OBJECT
        utils.write_metadata(os.path.join(self.rootdir, obj), metadata)

    def test_rmobjdir_exceptions(self):
        # ENOENT
        def do_rmdir(a):
            e = OSError()
            e.errno = errno.ENOENT
            raise e

        with mock.patch.object(utils, "do_rmdir", do_rmdir):
            assert not utils.rmobjdir("a")

        # NOT ENOTEMPTY
        def do_rmdir(a):
            e = OSError()
            e.errno = errno.ENOEXEC
            raise e

        with mock.patch.object(utils, "do_rmdir", do_rmdir):
            try:
                utils.rmobjdir("a")
            except OSError:
                pass
            else:
                self.fail("OSError waited")

    def test_rmobjdir_removing_files(self):
        self.assertFalse(utils.rmobjdir(self.rootdir))

        # Remove the files
        for f in self.files:
            os.unlink(os.path.join(self.rootdir, f))

        self.assertTrue(utils.rmobjdir(self.rootdir))

    def test_rmobjdir_removing_dirs(self):
        self.assertFalse(utils.rmobjdir(self.rootdir))

        # Remove the files
        for f in self.files:
            os.unlink(os.path.join(self.rootdir, f))

        self._set_dir_object(self.dirs[0])
        self.assertFalse(utils.rmobjdir(self.rootdir))
        self._clear_dir_object(self.dirs[0])
        self.assertTrue(utils.rmobjdir(self.rootdir))

    def test_rmobjdir_metadata_errors(self):
        def _mock_rm(path):
            print("_mock_rm-metadata_errors(%s)" % path)
            if path.endswith("dir3"):
                raise OSError(13, "foo")
            return {}

        _orig_rm = utils.read_metadata
        utils.read_metadata = _mock_rm
        try:
            try:
                utils.rmobjdir(self.rootdir)
            except OSError:
                pass
            else:
                self.fail("Expected OSError")
        finally:
            utils.read_metadata = _orig_rm

    def test_rmobjdir_metadata_enoent(self):
        def _mock_rm(path):
            print("_mock_rm-metadata_enoent(%s)" % path)
            shutil.rmtree(path)
            raise SwiftOnFileSystemIOError(errno.ENOENT, os.strerror(errno.ENOENT))

        # Remove the files
        for f in self.files:
            os.unlink(os.path.join(self.rootdir, f))

        _orig_rm = utils.read_metadata
        utils.read_metadata = _mock_rm
        try:
            try:
                self.assertTrue(utils.rmobjdir(self.rootdir))
            except IOError:
                self.fail("Unexpected IOError")
            else:
                pass
        finally:
            utils.read_metadata = _orig_rm

    def test_rmobjdir_rmdir_enoent(self):
        seen = [0]
        _orig_rm = utils.do_rmdir

        def _mock_rm(path):
            print("_mock_rm-rmdir_enoent(%s)" % path)
            if path == self.rootdir and not seen[0]:
                seen[0] = 1
                raise OSError(errno.ENOTEMPTY, os.strerror(errno.ENOTEMPTY))
            else:
                shutil.rmtree(path)
                raise OSError(errno.ENOENT, os.strerror(errno.ENOENT))

        # Remove the files
        for f in self.files:
            os.unlink(os.path.join(self.rootdir, f))

        utils.do_rmdir = _mock_rm
        try:
            try:
                self.assertTrue(utils.rmobjdir(self.rootdir))
            except OSError:
                self.fail("Unexpected OSError")
            else:
                pass
        finally:
            utils.do_rmdir = _orig_rm

    def test_rmobjdir_rmdir_error(self):
        seen = [0]
        _orig_rm = utils.do_rmdir

        def _mock_rm(path):
            print("_mock_rm-rmdir_enoent(%s)" % path)
            if path == self.rootdir and not seen[0]:
                seen[0] = 1
                raise OSError(errno.ENOTEMPTY, os.strerror(errno.ENOTEMPTY))
            else:
                raise OSError(errno.EACCES, os.strerror(errno.EACCES))

        # Remove the files
        for f in self.files:
            os.unlink(os.path.join(self.rootdir, f))

        utils.do_rmdir = _mock_rm
        try:
            try:
                utils.rmobjdir(self.rootdir)
            except OSError:
                pass
            else:
                self.fail("Expected OSError")
        finally:
            utils.do_rmdir = _orig_rm

    def test_rmobjdir_files_left_in_top_dir(self):
        seen = [0]
        _orig_rm = utils.do_rmdir

        def _mock_rm(path):
            print("_mock_rm-files_left_in_top_dir(%s)" % path)
            if path == self.rootdir:
                if not seen[0]:
                    seen[0] = 1
                    raise OSError(errno.ENOTEMPTY, os.strerror(errno.ENOTEMPTY))
                else:
                    return _orig_rm(path)
            else:
                shutil.rmtree(path)
                raise OSError(errno.ENOENT, os.strerror(errno.ENOENT))

        # Remove the files, leaving the ones at the root
        for f in self.files:
            if f.startswith("dir"):
                os.unlink(os.path.join(self.rootdir, f))

        utils.do_rmdir = _mock_rm
        try:
            try:
                self.assertFalse(utils.rmobjdir(self.rootdir))
            except OSError:
                self.fail("Unexpected OSError")
            else:
                pass
        finally:
            utils.do_rmdir = _orig_rm

    def test_rmobjdir_error_final_rmdir(self):
        seen = [0]
        _orig_rm = utils.do_rmdir

        def _mock_rm(path):
            print("_mock_rm-files_left_in_top_dir(%s)" % path)
            if path == self.rootdir:
                if not seen[0]:
                    seen[0] = 1
                    raise OSError(errno.ENOTEMPTY, os.strerror(errno.ENOTEMPTY))
                else:
                    raise OSError(errno.EACCES, os.strerror(errno.EACCES))
            else:
                shutil.rmtree(path)
                raise OSError(errno.ENOENT, os.strerror(errno.ENOENT))

        # Remove the files, leaving the ones at the root
        for f in self.files:
            os.unlink(os.path.join(self.rootdir, f))

        utils.do_rmdir = _mock_rm
        try:
            try:
                utils.rmobjdir(self.rootdir)
            except OSError:
                pass
            else:
                self.fail("Expected OSError")
        finally:
            utils.do_rmdir = _orig_rm

    def test_is_tmp_object(self):
        name = ".object." + uuid.uuid4().hex
        assert utils.is_tmp_obj(name)

        name = "/tmp/ok/.object." + uuid.uuid4().hex
        assert utils.is_tmp_obj(name)

        name = "/tmp/ok/myobject"
        assert not utils.is_tmp_obj(name)

        name = "myobject"
        assert not utils.is_tmp_obj(name)

    def test_get_user_uid_gid(self):
        with mock.patch("swiftonfile.swift.common.utils.pwd.getpwnam") as getpwnam:
            utils.get_user_uid_gid("test")
            getpwnam.assert_called_with("test")

    def test_get_group_gid(self):
        with mock.patch("swiftonfile.swift.common.utils.grp.getgrnam") as getgrnam:
            utils.get_group_gid("test")
            getgrnam.assert_called_with("test")
